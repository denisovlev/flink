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

package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Example illustrating iterations in Flink streaming.
 * <p> The program sums up random numbers and counts additions
 * it performs to reach a specific threshold in an iterative streaming fashion. </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>streaming iterations,
 * <li>buffer timeout to enhance latency,
 * <li>directed outputs.
 * </ul>
 * </p>
 */
public class DeadlockingIterateExample {

	private static final int BOUND = 100;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up input for the stream of integer pairs

		// obtain execution environment and set setBufferTimeout to 1 to enable
		// continuous flushing of the output buffers (lowest latency)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setBufferTimeout(1);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// create input stream of integer pairs
		DataStream<Integer> inputStream;
		if (params.has("input")) {
			inputStream = env.readTextFile(params.get("input")).map(new NumberInputMap());
		} else {
			System.out.println("Executing Iterate example with default input data set.");
			System.out.println("Use --input to specify file input.");
			inputStream = env.addSource(new NumberSource());
		}

		// create an iterative data stream from the input with 5 second timeout
		IterativeStream<Integer> it = inputStream.map(new InputMap()).iterate(5000);

		// apply the step function to get the next Fibonacci number
		// increment the counter and split the output with the output selector
		SplitStream<Integer> step = it.map(new Step()).split(new MySelector());

		// close the iteration by selecting the tuples that were directed to the
		// 'iterate' channel in the output selector
		it.closeWith(step.select("iterate"));

		//		SingleOutputStreamOperator<Integer> step = it.map(new Step());
		//		it.closeWith(step);

		// to produce the final output select the tuples directed to the
		// 'output' channel then get the input pairs that have the greatest iteration counter
		// on a 1 second sliding window
		DataStream<Integer> numbers = step.select("output").map(new OutputMap());

		// emit results
		if (params.has("output")) {
			numbers.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			numbers.print();
		}

		// execute the program
		env.execute("Streaming Iteration Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class NumberSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int number = 0;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {

			while (isRunning) {
				if (number % 10000 == 0) {
					//Print every 10,000th number (just so we can observe when numbers are being generated)
					System.out.println("Generated " + number);
				}

				if (number > 3000000) cancel();

				ctx.collect(number++);
//				Thread.sleep(1L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class NumberInputMap implements MapFunction<String, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(String value) throws Exception {
			return Integer.parseInt(value);
		}
	}

	public static class InputMap implements MapFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Integer value) throws Exception {
//			System.out.println("Input " + value);
			return value;
		}
	}

	public static class Step implements MapFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Integer value) throws Exception {
//			System.out.println("Step: " + value + " to " + value);
			int remainder = value % 10000;
			if(remainder > 2000) remainder = 2000;
			return (value - remainder);
		}
	}

	/**
	 * OutputSelector testing which tuple needs to be iterated again.
	 */
	public static class MySelector implements OutputSelector<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Integer value) {
			List<String> output = new ArrayList<>();

			// Send every 1000th number to the output (just so we can observe if the iteration is still running)
			if (value % 10000 != 0) {
//				System.out.println("Selector " + value + " sending to iterate");
				output.add("iterate");
			} else {
//				System.out.println("Selector " + value + " sending to output");
				output.add("output");
			}
			return output;
		}
	}

	/**
	 * Giving back the input pair and the counter.
	 */
	public static class OutputMap implements MapFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Integer value) throws Exception {
//			System.out.println("Output: " + value);
			return value;
		}
	}

}
