package org.apache.flink.streaming.examples.iteration;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This implementation uses streaming iterations to asynchronously merge state among partitions.
 * For a single-pass implementation, see {@link }.
 */
public class IterativeCC implements ProgramDescription {


	private static final Logger LOG = LoggerFactory.getLogger(IterativeCC.class);
	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<Tuple4<Long, Long, Long, Long>> edges = getEdgesDataSet(env);
//        edges.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.milliseconds(1))).max(0).print();
		IterativeStream<Tuple4<Long, Long, Long, Long>> iteration = edges.iterate();

		SplitStream<Tuple4<Long, Long, Long, Long>> something =
			iteration
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Long, Long>>() {

					@Override
					public long extractAscendingTimestamp(Tuple4<Long, Long, Long, Long> record) {
						return new Timestamp(System.currentTimeMillis()).getTime();
					}
				})
				.keyBy((a) -> {
					return a.f3;
				})
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.process(new MyProcessWindowFunction())
				.split(new MySelector());


//		iteration.closeWith(something.select("iterate"));

		DataStream<Tuple4<Long, Long, Long, Long>> result = iteration.closeWith(
			something.select("iterate")
		);

		DataStream<Tuple4<Long, Long, Long, Long>> output = something.select("output").map(
			new MapFunction<Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>>() {
				@Override
				public Tuple4<Long, Long, Long, Long> map(Tuple4<Long, Long, Long, Long> a) throws Exception {
					return a;
				}
			}
		);

		result.print();

//		edges.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Long, Long>>() {
//
//			@Override
//			public long extractAscendingTimestamp(Tuple4<Long, Long, Long, Long> record) {
//				return new Timestamp(System.currentTimeMillis()).getTime();
//			}
//		}).keyBy((a) -> {
//			return a.f3;
//		})
//			.window(TumblingEventTimeWindows.of(Time.minutes(5)))
//			.process(new MyProcessWindowFunction())
//			result.print();

//        IterativeStream<Tuple4<Long, Long, Long, Long>> iteration = edges.iterate(5000);


		env.execute("Streaming Connected Components");
	}

	public static class MySelector implements OutputSelector<Tuple4<Long, Long, Long, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple4<Long, Long, Long, Long> value) {
			List<String> output = new ArrayList<>();
			if(value.f0 == 0 && value.f1 == 0 && value.f2 == 0 && value.f3 == 0) {
				output.add("output");
			}
			else{
				output.add("iterate");
//				System.out.println("selector: "+value);
			}
			return output;
		}
	}

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {
		HashMap<Long, HashSet<Long>> hashMap = new HashMap<>();
        @Override
        public void process(Long key, Context context, Iterable<Tuple4<Long, Long, Long, Long>> input, Collector<Tuple4<Long, Long, Long, Long>> out) {
            System.out.println("KEY: " + key);
        	HashSet<Long> allVertices = new HashSet<>();

			for(Tuple4<Long, Long, Long, Long> in: input) {
				allVertices.add(in.f0);
				allVertices.add(in.f1);
			}

			long newKey = key;
			ArrayList<Long> connected = new ArrayList<>();

			for(Map.Entry<Long, HashSet<Long>> entry: hashMap.entrySet()){
				for(Long in: allVertices){
					if(entry.getValue().contains(in)){
						newKey = Math.min(newKey, entry.getKey());
						connected.add(entry.getKey());
						break;
					}
				}
			}
			System.out.println("++==++");
			System.out.println(key +" "+newKey);
			System.out.println(Arrays.toString(connected.toArray()));
			System.out.println(hashMap.keySet());
			System.out.println("++==++");
			for(Long keyConnected: connected){

				System.out.println("keyConnected: "+keyConnected + " "+ hashMap.keySet());
				HashSet<Long> connectedSet = hashMap.remove(keyConnected);

				allVertices.addAll(connectedSet);
			}

			hashMap.put(newKey, allVertices);


			for(Long in: allVertices){
//				if(newKey != in)
					out.collect(new Tuple4<>(newKey, in, 1L, newKey));
			}
			out.collect(new Tuple4<>(0L, 0L, 0L, 0L));

        }
    }

	private static class MyWindowFunction<TumblingEventTimeWindows> implements WindowFunction<Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {

		@Override
		public void apply(Long key, TimeWindow window, Iterable<Tuple4<Long, Long, Long, Long>> values, Collector<Tuple4<Long, Long, Long, Long>> out) {
			long sum = 0L;
			for (Tuple4<Long, Long, Long, Long> value : values) {
				sum += value.f1;
			}

			out.collect(new Tuple4<Long, Long, Long, Long>(key, sum, 0L, 0L));
		}
	}

	private static class EdgeSource implements SourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		private Random rnd = new Random();

		private volatile boolean isRunning = true;
		private int counter = 0;

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

			while (isRunning) {
				int first = rnd.nextInt();
				int second = rnd.nextInt();

				ctx.collect(new Tuple2<>(first, second));
				counter++;
				Thread.sleep(50L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 1) {
				System.err.println("Usage: ConnectedComponentsExample <input edges path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
		} else {
			System.out.println("Executing ConnectedComponentsExample example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ConnectedComponentsExample <input edges path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataStream<Tuple4<Long, Long, Long, Long>> getEdgesDataSet(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return env.readTextFile(edgeInputPath )
				.map(new MapFunction<String, Tuple4<Long, Long, Long, Long>>() {
					@Override
					public Tuple4<Long, Long, Long, Long> map(String s) {
						String[] fields = s.split("\\t");
						long src = Long.parseLong(fields[0]);
						long trg = Long.parseLong(fields[1]);
						long newSource = Math.min(src, trg);
						long newDest = Math.max(src, trg);
						return new Tuple4<>(newSource, newDest, System.currentTimeMillis(), Math.min(src, trg));
					}
				});
		}

		return env.generateSequence(1, 10).flatMap(
			new FlatMapFunction<Long, Tuple4<Long, Long, Long, Long>>() {
				@Override
				public void flatMap(Long key, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
					for (int i = 1; i < 3; i++) {
						long target = key + i;
						out.collect(new Tuple4<>(key, target, System.currentTimeMillis(), Math.min(key, target)) );
					}
				}
			});
	}

	@Override
	public String getDescription() {
		return "Streaming Connected Components";
	}
}
