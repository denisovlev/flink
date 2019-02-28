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

package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class CheckCC {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			// get default test text data
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			text = env.fromElements(null);
		}

		FilterOperator<Tuple2<Integer, Integer>> cc = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(String line) throws Exception {
				String[] item = line.split("\\s+");
//				System.out.println(String.join(" -- ", item));
				Integer vid = Integer.parseInt(item[0]);
				// last tuple in CC output has no label?
				Integer label = -1;
				if(item.length >= 2)
					label = Integer.parseInt(item[1]);
				return new Tuple2<>(vid, label);
			}
		}).groupBy(0).minBy(1).groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<Integer,Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer>> out) throws Exception {
				Integer cnt = 0;
				Tuple2<Integer, Integer> v = null;
				for (Tuple2<Integer, Integer> value : values) {
					cnt += 1;
					v = value;
				}
				if (v != null) out.collect(new Tuple2<>(v.f1, cnt));
				else { out.collect(new Tuple2<>(-1, -1)); }
			}
		}).filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> group) throws Exception {
				return group.f1 > 0;
			}
		});

		// emit result
		if (params.has("output")) {
			cc.writeAsCsv(params.get("output"), "\n", " ");
			// execute program
			env.execute("WordCount Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			cc.print();
		}

	}

}
