/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A special {@link StreamTask} that is used for executing feedback edges. This is used in
 * combination with {@link StreamIterationTail}.
 */
@Internal
public class StreamIterationHead<OUT> extends OneInputStreamTask<OUT, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;

	private UpstreamLogger<OUT> upstreamLogger;

	private Object lock;

	public StreamIterationHead(Environment env) {
		super(env);
	}

	// ------------------------------------------------------------------------

	private StreamElementSerializer<OUT> createRecordSerializer() {
		StreamTask containingTask = this;
		final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);
		Environment taskEnvironment = this.getEnvironment();
		StreamConfig upStreamConfig = chainedConfigs.values().iterator().next();
		TypeSerializer<OUT> outSerializer = upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserClassLoader());
		return new StreamElementSerializer<OUT>(outSerializer);
	}

	@Override
	protected void run() throws Exception {

		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}

		final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId ,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());

		final long iterationWaitTime = getConfiguration().getIterationWaitTime();
		final boolean shouldWait = iterationWaitTime > 0;

		StreamElementSerializer<OUT> streamElementSerializer = createRecordSerializer();
		StreamElementOrEventSerializer<OUT> serializer = new StreamElementOrEventSerializer<OUT>(streamElementSerializer);
		// the number of records spillable queue can hold is a tradeoff between memory and performance
		SpillableQueue<Either<StreamRecord<OUT>, CheckpointBarrier>> dataChannel = new SpillableQueue<>(1024 * 8, serializer);

		// offer the queue for the tail
		BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work
		try {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<OUT>[] outputs = (RecordWriterOutput<OUT>[]) getStreamOutputs();

			// If timestamps are enabled we make sure to remove cyclic watermark dependencies
			if (isSerializingTimestamps()) {
				for (RecordWriterOutput<OUT> output : outputs) {
					output.emitWatermark(new Watermark(Long.MAX_VALUE));
				}
			}

			synchronized (lock) {
				//emit in-flight events in the upstream log upon recovery
				for (StreamRecord<OUT> rec : upstreamLogger.getReplayLog()) {
					for (RecordWriterOutput<OUT> output : outputs) {
						output.collect(rec);
					}
				}
				upstreamLogger.clearLog();
			}


			while (running) {

				Either<StreamRecord<OUT>, CheckpointBarrier> nextRecord = shouldWait ?
					dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
					dataChannel.take();

				synchronized (lock) {
					if (nextRecord != null) {
						if (nextRecord.isLeft()) {
							//log in-transit records whose effects are not part of ongoing snapshots
							upstreamLogger.logRecord(nextRecord.left());

							for (RecordWriterOutput<OUT> output : outputs) {
								output.collect(nextRecord.left());
							}
						} else {
							//upon marker from tail (markers should loop back in FIFO order)
							checkpointState(new CheckpointMetaData(
								nextRecord.right().getId(), 
								nextRecord.right().getTimestamp()),
								nextRecord.right().getCheckpointOptions(),
								new CheckpointMetrics()
							);
							upstreamLogger.discardSlice();
						}
					}
					else {
						// done
						break;
					}
				}

			}
		}
		finally {
			// make sure that we remove the queue from the broker, to prevent a resource leak
			BlockingQueueBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	// ------------------------------------------------------------------------

	@Override
	public void init() {
		this.lock = getCheckpointLock();
		getConfiguration().setStreamOperator(new UpstreamLogger(getConfiguration(), createRecordSerializer()));
		List<StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>> streamRecordWriters =
			StreamTask.createStreamRecordWriters(configuration, getEnvironment());
		operatorChain = new OperatorChain<>(this, streamRecordWriters);
		this.upstreamLogger = (UpstreamLogger<OUT>) operatorChain.getHeadOperator();
	}

	@Override
	protected void cleanup() throws Exception {
		// does not hold any resources, no cleanup necessary
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {

		//create a buffer for logging the initiated snapshot and broadcast a barrier for it
		synchronized (getCheckpointLock()) {
			//	we create log slices to support multiple concurrent checkpoints
			// 	see more https://github.com/apache/flink/pull/1668#issuecomment-266572177
			upstreamLogger.createSlice(String.valueOf(checkpointMetaData.getCheckpointId()));
			operatorChain.broadcastCheckpointBarrier(checkpointMetaData.getCheckpointId(), checkpointMetaData.getTimestamp(), checkpointOptions);
		}

		return true;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the shared blocking
	 * queue for the back channel. The identification string is unique per parallel head/tail pair
	 * per iteration per job.
	 *
	 * @param jid The job ID.
	 * @param iterationID The id of the iteration in the job.
	 * @param subtaskIndex The parallel subtask number
	 * @return The identification string.
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}
}
