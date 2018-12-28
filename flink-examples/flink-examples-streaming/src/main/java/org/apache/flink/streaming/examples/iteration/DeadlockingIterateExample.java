package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DeadlockingIterateExample {
	public static void main(String[] args) throws Exception {
		long fastPeriodMs = 5 * 1000;
		long slowPeriodMs = 5 * 1000;
		long fastPeriodSpeed = 0;
		long slowPeriodSpeed = 100;
		int cycles = 5;
		int pctSendToOutput = 1;

		if (args.length >= 6) {
			fastPeriodMs = Long.parseLong(args[0]);
			slowPeriodMs = Long.parseLong(args[1]);
			fastPeriodSpeed = Long.parseLong(args[2]);
			slowPeriodSpeed = Long.parseLong(args[3]);
			cycles = Integer.parseInt(args[4]);
			pctSendToOutput = Integer.parseInt(args[5]);
		}

		Random random = new Random();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);

		DataStream<Long> inputStream = env.addSource(
			new NumberSource(fastPeriodMs, slowPeriodMs, fastPeriodSpeed, slowPeriodSpeed, cycles));

		IterativeStream<Long> iteration = inputStream.map(value -> value).iterate();
		DataStream<Long> iterationBody = iteration.map(value -> value);

		int finalPctSendToOutput = pctSendToOutput;
		SplitStream<Long> splitStream = iterationBody.split((OutputSelector<Long>) value -> {
			List<String> output = new ArrayList<>();
			// Send some numbers to output (set in pctSendToOutput), the rest back to the iteration
			if (random.nextInt(100) < finalPctSendToOutput) {
				output.add("output");
			} else {
				output.add("iterate");
			}
			return output;
		});

		// Send 'iterate' back to the feedback loop
		iteration.closeWith(splitStream.select("iterate"));

		// Send 'output' to output
		DataStream<Long> output = splitStream.select("output");

		output.print();

		env.execute("Deadlocking Iteration Example");
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
	 */
	private static class NumberSource implements SourceFunction<Long> {
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
		public void run(SourceContext<Long> ctx) throws Exception {
			List<Tuple2<Long, Long>> executionPeriods = computeExecutionPeriods();

			while (isRunning) {
				ctx.collect(number++);
				System.out.print("*");

				//				//TEMP
				//				if (number > 5000) {
				//					isRunning = false;
				//					System.out.println("testing upper bound reached, max number generated " + (number - 1));
				//				}

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
