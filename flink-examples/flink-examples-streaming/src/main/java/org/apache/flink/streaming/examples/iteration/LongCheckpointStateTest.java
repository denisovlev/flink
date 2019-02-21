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

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class LongCheckpointStateTest {
	private static final Logger LOG = LoggerFactory.getLogger(LongCheckpointStateTest.class);
	private static final String TOUCH_FILE = System.getProperty("java.io.tmpdir") + "/LongCheckpointStateTest.marker";

	public static void main(String[] args) throws Exception {
		new LongCheckpointStateTest(args);
	}

	public LongCheckpointStateTest(String[] args) throws Exception {
		int checkpointInterval = 2 * 60000;
		long endNumber = 100000;
		int probability = 5000;
		int minExceptionElapseTime = 60000;
		int speed = 1;
		int parallelism = 1;

		if (args.length >= 6) {
			checkpointInterval = Integer.parseInt(args[0]);
			endNumber = Long.parseLong(args[1]);
			probability = Integer.parseInt(args[2]);
			minExceptionElapseTime = Integer.parseInt(args[3]);
			speed = Integer.parseInt(args[4]);
			parallelism = Integer.parseInt(args[5]);
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);
		env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
		env.getCheckpointConfig().setForceCheckpointing(true);
		env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/feedbacklooptempdir/checkpoint", false));
		env.setParallelism(parallelism);

		DataStream<Tuple2<Long, Boolean>> inputStream = env.addSource(new NumberSource(endNumber, probability, minExceptionElapseTime, speed));
		IterativeStream<Tuple2<Long, Boolean>> iteration = inputStream.map(LongCheckpointStateTest::noOpMap).iterate();
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

		env.execute("LongCheckpointStateTest");
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
		private int probability;
		private int minExceptionElapseTime;
		private int speed;

		public NumberSource(long endNumber, int probability, int minExceptionElapseTime, int speed) {
			this.endNumber = endNumber;
			this.probability = probability;
			this.minExceptionElapseTime = minExceptionElapseTime;
			this.speed = speed;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Boolean>> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();

			// Delete any existing touch files
			resetTouchFile();

			while (isRunning) {
				if (number <= endNumber) {
					synchronized (lock) {
						ctx.collect(new Tuple2<Long, Boolean>(number++, false));
					}

					Thread.sleep(speed); //cannot remove thread.sleep coz number generation will be too fast that it will trigger RTE before the first checkpoint (i.e. no recovery from checkpoint happens)

					Random random = new Random();
					if (random.nextInt(probability) == 1) { // probability of RTE needs to be low enough that it will be triggered after the first checkpoint
						long lastExceptionTime = getLastExceptionTime();
						long diff = System.currentTimeMillis() - lastExceptionTime;
						if (lastExceptionTime == 0 || diff >= minExceptionElapseTime) { // 1 minute
							LOG.debug("*********THROW RTE*********" + lastExceptionTime + " " + diff);

							FileOutputStream fos = new FileOutputStream(TOUCH_FILE);
							DataOutputStream dos = new DataOutputStream(fos);
							dos.writeLong(System.currentTimeMillis());

							dos.close();

							throw new RuntimeException();
						}
					}
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

	private static long getLastExceptionTime() {
		long timestamp = 0;
		try {
			FileInputStream fis = new FileInputStream(TOUCH_FILE);
			DataInputStream dis = new DataInputStream(fis);
			timestamp = dis.readLong();
			dis.close();
		} catch (IOException e) {
		}
		return timestamp;
	}

	private static void resetTouchFile() {
		File f = new File(TOUCH_FILE);
		LOG.debug("deleted touch file..." + f.getAbsolutePath());
		f.delete();
	}

	private class ChecksumChecker implements MapFunction<Tuple2<Long, Boolean>, Tuple2<Long, Boolean>>, ListCheckpointed<Long> {
		private long sum = 0;

		@Override
		public Tuple2<Long, Boolean> map(Tuple2<Long, Boolean> tuple) throws Exception {
			if (tuple.f1) {
				// if tuple already entered the loop in a previous iteration
				sum = sum + tuple.f0;
				LOG.debug("tuple={} sum={}", tuple.f0, sum);
				tuple.f1 = false;
			} else {
				// first time entering iterationBody
				tuple.f1 = true;
			}
			return tuple;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.debug("ChecksumChecker save tuple={}", sum);
			return Collections.singletonList(sum);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			for (Long s : state) {
				sum = s;
				LOG.debug("ChecksumChecker load tuple={}", sum);
			}
		}
	}

}
