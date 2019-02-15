package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CheckpointStateTest {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointStateTest.class);

	public static void main(String[] args) throws Exception {
		new CheckpointStateTest();
	}

	public CheckpointStateTest() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);
		env.getCheckpointConfig().setCheckpointInterval(1000);
		env.getCheckpointConfig().setForceCheckpointing(true);
		env.setStateBackend(new FsStateBackend("file:///stateFile/", false));
		env.setParallelism(1);

		DataStream<Tuple2<Long, Boolean>> inputStream = env.addSource(new NumberSource());
		IterativeStream<Tuple2<Long, Boolean>> iteration = inputStream.map(CheckpointStateTest::noOpMap).iterate();
		DataStream<Tuple2<Long, Boolean>> iterationBody = iteration.map(new ChecksumChecker());

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

		env.execute("CheckpointStateTest");
	}

	private static Tuple2<Long, Boolean> noOpMap(Tuple2<Long, Boolean> value) {
		return value;
	}

	/**
	 * Tuple2(number, boolean if number has entered the iteration body before)
	 */
	private class NumberSource implements SourceFunction<Tuple2<Long, Boolean>>, CheckpointedFunction {
		private boolean isRunning = true;
		private long number = 0;
		private transient ListState<Long> checkpointedNumber;

		@Override
		public void run(SourceContext<Tuple2<Long, Boolean>> ctx) throws Exception {
			while (isRunning) {
				if (number <= 10000) {
					ctx.collect(new Tuple2<Long, Boolean>(number++, false));
				}

				Thread.sleep(1);

				Random random = new Random();
				if (random.nextInt(100) < 1) {
					LOG.debug("*********THROW RTE*********");
					throw new RuntimeException();
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			this.checkpointedNumber.clear();
			this.checkpointedNumber.add(number);
			LOG.debug("NumberSource save tuple={}", this.number);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			this.checkpointedNumber = context.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("NumberSource count", Long.class));

			if (context.isRestored()) {
				for (Long item : this.checkpointedNumber.get()) {
					this.number = item;
					LOG.debug("NumberSource load tuple={}", this.number);
				}
			}
		}
	}

	private class ChecksumChecker implements MapFunction<Tuple2<Long, Boolean>, Tuple2<Long, Boolean>>, CheckpointedFunction {
		private long sum = 0;
		private transient ListState<Long> checkpointedSum;

		@Override
		public Tuple2<Long, Boolean> map(Tuple2<Long, Boolean> tuple) throws Exception {
			if (tuple.f1) {
				// if tuple already entered the loop in a previous iteration

				// TODO
				//		sum = sum ^ tuple.f0;

				sum = sum + tuple.f0;
				LOG.debug("tuple={} added to current sum={}", tuple.f0, sum);
				tuple.f1 = false;
			} else {
				// first time entering iterationBody
				tuple.f1 = true;
			}
			return tuple;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			this.checkpointedSum.clear();
			this.checkpointedSum.add(sum);
			LOG.debug("ChecksumChecker save tuple={}", this.sum);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			this.checkpointedSum = context.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("ChecksumChecker count", Long.class));

			if (context.isRestored()) {
				for (Long item : this.checkpointedSum.get()) {
					this.sum = item;
					LOG.debug("ChecksumChecker load tuple={}", this.sum);
				}
			}
		}
	}
}
