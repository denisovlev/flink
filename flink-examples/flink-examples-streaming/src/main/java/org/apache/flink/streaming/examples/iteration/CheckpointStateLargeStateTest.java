package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CheckpointStateLargeStateTest {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointStateLargeStateTest.class);
	private static final String TOUCH_FILE = System.getProperty("java.io.tmpdir") + "/CheckpointStateOneSourceTest.marker";

	public static void main(String[] args) throws Exception {
		new CheckpointStateLargeStateTest(args);
	}

	public CheckpointStateLargeStateTest(String[] args) throws Exception {
		int checkpointInterval = 5000;
		long endNumber = 100000;
		int probability = 5000;
		int speed = 1;
		int parallelism = 2;
		int stateSizeBytes = 8;

		if (args.length >= 6) {
			checkpointInterval = Integer.parseInt(args[0]);
			endNumber = Long.parseLong(args[1]);
			probability = Integer.parseInt(args[2]);
			speed = Integer.parseInt(args[3]);
			parallelism = Integer.parseInt(args[4]);

			String stateSizeArg = args[5];
			char unit = stateSizeArg.charAt(stateSizeArg.length() - 1);
			switch (unit) {
				case 'M':
					stateSizeBytes = Integer.parseInt(stateSizeArg.substring(0, stateSizeArg.length() - 1)) * 1024 * 1024;
					break;
				case 'K':
					stateSizeBytes = Integer.parseInt(stateSizeArg.substring(0, stateSizeArg.length() - 1)) * 1024;
					break;
				default:
					stateSizeBytes = Integer.parseInt(stateSizeArg);
			}
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);
		env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
		env.getCheckpointConfig().setForceCheckpointing(true);
		//		env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/feedbacklooptempdir/checkpoint", false));
		env.setStateBackend(new FsStateBackend("hdfs://ibm-power-1.dima.tu-berlin.de:44000/user/hadoop/flink-loop-temp/checkpointtest/checkpoint", false));

		env.setParallelism(1);

		int finalParallelism = parallelism;
		DataStream<Tuple2<Long, Boolean>> inputStream = env.addSource(new NumberSource(endNumber, probability, speed));
		IterativeStream<Tuple2<Long, Boolean>> iteration = inputStream
			.map(CheckpointStateLargeStateTest::noOpMap)
			.setParallelism(finalParallelism)
			.iterate();
		DataStream<Tuple2<Long, Boolean>> iterationBody = iteration.map(new ChecksumChecker(stateSizeBytes)).setParallelism(finalParallelism);

		SplitStream<Tuple2<Long, Boolean>> splitStream = iterationBody.split((OutputSelector<Tuple2<Long, Boolean>>) tuple -> {
			List<String> output = new ArrayList<>();
			if (tuple.f1) {
				output.add("iterate");
			} else {
				// if tuple has entered body before, output
				output.add("output");
			}
			return output;
		});

		iteration.closeWith(splitStream.select("iterate"));
		splitStream.select("output").print();

		env.execute("CheckpointStateOneSourceTest");
	}

	private static Tuple2<Long, Boolean> noOpMap(Tuple2<Long, Boolean> value) {
		return value;
	}

	/**
	 * Tuple2(number, boolean if number has entered the iteration body before)
	 */
	private class NumberSource extends RichParallelSourceFunction<Tuple2<Long, Boolean>> implements ListCheckpointed<Long> {
		private boolean isRunning = true;
		private long number = 0;
		private long endNumber;
		private int speed;

		public NumberSource(long endNumber, int probability, int speed) {
			this.endNumber = endNumber;
			this.speed = speed;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Boolean>> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();

			while (isRunning) {
				if (number <= endNumber) {
					synchronized (lock) {
						ctx.collect(new Tuple2<Long, Boolean>(number++, false));
					}

					Thread.sleep(speed); //cannot remove thread.sleep coz number generation will be too fast that it will trigger RTE before the first checkpoint (i.e. no recovery from checkpoint happens)
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.debug("NumberSource save tuple={}", number);
			return Collections.singletonList(number);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			for (Long s : state) {
				number = s;
				LOG.debug("NumberSource load tuple={}", number);
			}
		}
	}

	private class ChecksumChecker implements MapFunction<Tuple2<Long, Boolean>, Tuple2<Long, Boolean>>, ListCheckpointed<Byte> {
		private int stateSizeBytes;
		private byte[] sumBytes;

		public ChecksumChecker(int stateSizeBytes) {
			this.stateSizeBytes = stateSizeBytes;
			this.sumBytes = new byte[stateSizeBytes];
		}

		@Override
		public Tuple2<Long, Boolean> map(Tuple2<Long, Boolean> tuple) throws Exception {
			if (tuple.f1) {
				// if tuple already entered the loop in a previous iteration
				long newSum = getSum() + tuple.f0;
				setSum(newSum);
				LOG.debug("tuple={} sum={}", tuple.f0, newSum);
				tuple.f1 = false;
			} else {
				// first time entering iterationBody
				tuple.f1 = true;
			}
			return tuple;
		}

		@Override
		public List<Byte> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.debug("ChecksumChecker save tuple={}", getSum());
			List<Byte> bytes = new ArrayList<>();
			for (byte b : sumBytes) {
				bytes.add(b);
			}
			return bytes;
		}

		@Override
		public void restoreState(List<Byte> state) throws Exception {
			//Sanity check - size of list should be same as stateSizeBytes. In any case, still load it, coz we just need the first 8 bytes (long).
			if (state.size() != stateSizeBytes) {
				LOG.warn("State size from checkpoint ({} bytes) is not equal to expected state size ({} bytes)", state.size(), stateSizeBytes);
			}

			for (int i = 0; i < sumBytes.length; i++) {
				sumBytes[i] = state.get(i);
			}

			LOG.debug("ChecksumChecker load tuple={}", getSum());
		}

		private long getSum() {
			ByteBuffer bb = ByteBuffer.wrap(sumBytes);
			return bb.getLong();
		}

		private void setSum(long n) {
			ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
			bb.putLong(n);
			byte[] byteArray = bb.array();

			for (int i = 0; i < byteArray.length; i++) {
				sumBytes[i] = byteArray[i];
			}
		}
	}
}
