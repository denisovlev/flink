package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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

public class DeadlockingIterateExample {

	private static final Logger LOG = LoggerFactory.getLogger(DeadlockingIterateExample.class);

	public static void main(String[] args) throws Exception {
		new DeadlockingIterateExample(args);
	}

	public DeadlockingIterateExample(String[] args) throws Exception {
		long fastPeriodMs = 5 * 1000;
		long slowPeriodMs = 5 * 1000;
		long fastPeriodSpeed = 0;
		long slowPeriodSpeed = 100;
		int cycles = 5;
		int pctSendToOutput = 1;
		int fixedIterations = 0;

		if (args.length >= 6) {
			fastPeriodMs = Long.parseLong(args[0]);
			slowPeriodMs = Long.parseLong(args[1]);
			fastPeriodSpeed = Long.parseLong(args[2]);
			slowPeriodSpeed = Long.parseLong(args[3]);
			cycles = Integer.parseInt(args[4]);
			pctSendToOutput = Integer.parseInt(args[5]);
			fixedIterations = Integer.parseInt(args[6]);
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);
		env.getCheckpointConfig().setCheckpointInterval(5000);
		env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/feedbacklooptempdir/checkpoint", false));

		DataStream<Tuple4<Long, Long, Long, Long>> inputStream = env.addSource(
			new NumberSource(fastPeriodMs, slowPeriodMs, fastPeriodSpeed, slowPeriodSpeed, cycles));

		IterativeStream<Tuple4<Long, Long, Long, Long>> iteration = inputStream.map(DeadlockingIterateExample::noOpMap).iterate();
		DataStream<Tuple4<Long, Long, Long, Long>> iterationBody = iteration.map(DeadlockingIterateExample::incrementIterations);

		SplitStream<Tuple4<Long, Long, Long, Long>> splitStream;
		if (fixedIterations > 0) {
			int finalFixedIterations = fixedIterations;
			splitStream = iterationBody.split((OutputSelector<Tuple4<Long, Long, Long, Long>>) value -> {
				List<String> output = new ArrayList<>();
				// Send number to output if it already reached fixed number of iterations
				if (value.f2 >= finalFixedIterations) {
					output.add("output");
				} else {
					output.add("iterate");
				}
				return output;
			});
		} else {
			Random random = new Random();
			int finalPctSendToOutput = pctSendToOutput;
			splitStream = iterationBody.split((OutputSelector<Tuple4<Long, Long, Long, Long>>) value -> {
				List<String> output = new ArrayList<>();
				// Send some numbers to output (set in pctSendToOutput), the rest back to the iteration
				if (random.nextInt(100) < finalPctSendToOutput) {
					output.add("output");
				} else {
					output.add("iterate");
				}
				return output;
			});
		}

		// Send 'iterate' back to the feedback loop
		iteration.closeWith(splitStream.select("iterate"));

		// Send 'output' to output
		DataStream<Tuple4<Long, Long, Long, Long>> output = splitStream.select("output").map(DeadlockingIterateExample::logDuration);

		output.filter((new FilterFunction<Tuple4<Long, Long, Long, Long>>() {
			@Override
			public boolean filter(Tuple4<Long, Long, Long, Long> value) throws Exception {
				return value.f0 < 0;
			}
		})).print();

		env.execute("Deadlocking Iteration Example");
	}

	private static Tuple4<Long, Long, Long, Long> noOpMap(Tuple4<Long, Long, Long, Long> value) {
		Random random = new Random();
		// fail job randomly
		if (random.nextInt(4000000) <= 1) throw new RuntimeException("test test test");
		return value;
	}

	private static Tuple4<Long, Long, Long, Long> logDuration(Tuple4<Long, Long, Long, Long> value) {

		if (LOG.isDebugEnabled()) {
			long now = System.currentTimeMillis();
			long totalTimeFromSource = now - value.f1;
			long totalTimeFromLoopEntry = now - value.f3;

//			LOG.debug("{},{},{},{},{},{},{}",
//				value.f0,                           // the number
//				value.f2,                           // number of iterations
//				value.f1,                           // number generation timestamp
//				value.f3,                           // loop entry timestamp
//				now,                                // loop exit timestamp
//				totalTimeFromSource / value.f2,     // duration/iteration (from source)
//				totalTimeFromLoopEntry / value.f2   // duration/iteration (from loop entry)
//			);
		}
		return value;
	}

	private static Tuple4<Long, Long, Long, Long> incrementIterations(Tuple4<Long, Long, Long, Long> value) {
		if (value.f3 == -1l) {
			value.f3 = System.currentTimeMillis();
		}

		value.f2 += 1;
		return value;
	}

	/**
	 * Number Generator (long) with speed variations
	 *
	 * The parameter fastPeriodMs provides the length of time (in ms) to generate numbers at speed specified in
	 * fastPeriodSpeed (by timer sleep, in ms). Likewise for slowPeriodMs and slowPeriodSpeed.
	 * These periods will be run in the # of cycles specified in the cycles parameter. 1 cycle = 1 fast, 1slow
	 * If cycles = -1, there is no speed variation (no sleep), and the number generation time is unbounded
	 * If *Speed is 0, Timer.sleep is not called at all.
	 * <p>
	 * Example: fastPeriodMs=5s, fastPeriodSpeed=0, slowPeriodMs=5s, slowPeriodSpeed=100ms, cycles=5
	 * One cycle is 10 seconds: the first 5 seconds, numbers are being generated continuously (no sleep), and the next 5 seconds, 1 number/100ms
	 * Five such cycles are executed, resulting in a total time of 50 seconds (5 cycles * 10s/cycle)
	 *
	 * The data type is a Tuple4 which is used as follows:
	 * f0 - the generated number
	 * f1 - timestamp of generation
	 * f2 - number of iterations (initialized to 0, incremented each pass through the iteration body)
	 * f3 - timestamp of first loop entry
	 */
	private class NumberSource implements SourceFunction<Tuple4<Long, Long, Long, Long>> {
		private volatile boolean isRunning = true;
		private long number = 0;
		private long fastPeriodMs;
		private long slowPeriodMs;
		private long fastPeriodSpeed;
		private long slowPeriodSpeed;
		private int cycles;

		public NumberSource(long fastPeriodMs, long slowPeriodMs, long fastPeriodSpeed, long slowPeriodSpeed, int cycles) {
			this.fastPeriodMs = fastPeriodMs;
			this.slowPeriodMs = slowPeriodMs;
			this.fastPeriodSpeed = fastPeriodSpeed;
			this.slowPeriodSpeed = slowPeriodSpeed;
			this.cycles = cycles;
		}

		private List<Tuple2<Long, Long>> computeExecutionPeriods() {
			if (cycles == -1) {
				// When cycles = -1, no cycles (generate numbers ad infinitum)
				return null;
			}

			List<Tuple2<Long, Long>> executionPeriods = new ArrayList<>();

			long t0 = System.currentTimeMillis();

			for (int i = 0; i < cycles; i++) {
				long cycleBaseline = t0 + (fastPeriodMs + slowPeriodMs) * i;
				executionPeriods.add(new Tuple2(cycleBaseline + fastPeriodMs, fastPeriodSpeed));
				executionPeriods.add(new Tuple2(cycleBaseline + fastPeriodMs + slowPeriodMs, slowPeriodSpeed));
			}

			//			System.out.println("EXECUTION PLAN " + t0);
			//			for (Tuple2<Long, Long> executionTime : executionPeriods) {
			//				System.out.println(executionTime);
			//			}

			return executionPeriods;
		}

		@Override
		public void run(SourceContext<Tuple4<Long, Long, Long, Long>> ctx) throws Exception {
			List<Tuple2<Long, Long>> executionPeriods = computeExecutionPeriods();

			while (isRunning) {
				ctx.collect(new Tuple4(number++, System.currentTimeMillis(), 0l, -1l));
//				System.out.print("*");

				if (executionPeriods == null) {
					// cycles was set to -1. do nothing, no sleep, no speed variation, no stopping
				} else if (executionPeriods.size() == 0) {
					// No more periods remaining, stop number generation
					System.out.println("Execution periods completed, number generation stopped. Last number generated: " + (number - 1));
					isRunning = false;
				} else {
					Tuple2<Long, Long> currentPeriod = executionPeriods.get(0);
					long currentTime = System.currentTimeMillis();

					if (currentTime < currentPeriod.f0) {
						// Check if current time is still within the current period (index 0), and sleep by the specified speed
						if (currentPeriod.f1 > 0) {
							Thread.sleep(currentPeriod.f1);
						}
					} else {
						// Otherwise, move on to the next period by removing the current period (index 0)
						synchronized (executionPeriods) {
							System.out.println("Expired exec time " + currentTime + " " + executionPeriods.get(0));
							executionPeriods.remove(0);
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
