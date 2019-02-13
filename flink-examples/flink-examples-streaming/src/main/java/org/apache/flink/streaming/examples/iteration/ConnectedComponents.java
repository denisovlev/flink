package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ConnectedComponents {
	private static final int WINDOW_SIZE = 1;
	public static void main(String args[]) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// edges reference [ (1,2), (2,1), (2,3), (3,2) ...]
		DataStream<Tuple2<Long, Long>> initEdge = env.addSource(new EdgeSource());
		DataStream<Tuple2<Long, Long>> inputEdge =
			initEdge
				.flatMap(new UndirectEdge())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Long>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Long, Long> record) {
						return new Timestamp(System.currentTimeMillis()+1000).getTime();
					}
				});
//		inputEdge.print();
		// initial vertices values: pair for id with 0 iteration [(1,1,0), (2,2,0), ..]
		DataStream<Tuple3<Long, Long, Long>> verticesWithInitialId = initEdge.map(new ExtractVertices<>());
//		verticesWithInitialId.print();
		// first step
		DataStream<Tuple4<Long, Long, Long, Boolean>> joinedResult =
			verticesWithInitialId
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Long, Long>>() {
					@Override
					public long extractAscendingTimestamp(Tuple3<Long, Long, Long> record) {
						return new Timestamp(System.currentTimeMillis()+1000).getTime();
					}
				})
				.join(inputEdge)
				.where(new KeySelector<Tuple3<Long,Long,Long>, Long>() {
					@Override
					public Long getKey(Tuple3<Long, Long, Long> value) throws Exception {
						return value.f1;
					}
				}).equalTo(new KeySelector<Tuple2<Long, Long>, Long>() {
					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE)))
				.apply(new JoinFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple4<Long, Long, Long, Boolean>>() {
				@Override
				public Tuple4<Long, Long, Long, Boolean> join(Tuple3<Long, Long, Long> first, Tuple2<Long, Long> second) throws Exception {

					Tuple4<Long, Long, Long, Boolean> tuple4 = new Tuple4<>(first.f0, Math.min(first.f1, second.f1), second.f1, (first.f1 > second.f1));
					System.out.println(tuple4);
					return tuple4;
				}
				})
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Long, Boolean>>() {

					@Override
					public long extractAscendingTimestamp(Tuple4<Long, Long, Long, Boolean> record) {
						return new Timestamp(System.currentTimeMillis()+1000).getTime();
					}
				});

		// define iteration
		IterativeStream<Tuple4<Long,Long, Long, Boolean>> it = joinedResult.iterate();

		// ----- Iteration start
		DataStream<Tuple2<Long, Long>> stream1 =
			it.map(new MapFunction<Tuple4<Long, Long, Long, Boolean>, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(Tuple4<Long, Long, Long, Boolean> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			})
			.keyBy(0)
			.minBy(1);

		DataStream<Tuple2<Long, Long>> stream2 =
			it.flatMap(new FlatMapFunction<Tuple4<Long, Long, Long, Boolean>, Tuple2<Long, Long>>() {
				@Override
				public void flatMap(Tuple4<Long, Long, Long, Boolean> value, Collector<Tuple2<Long, Long>> out) throws Exception {
					out.collect(new Tuple2<>(value.f0, value.f2));
//					out.collect(new Tuple2<>(value.f2, value.f1));
				}
			});

		DataStream<Tuple4<Long, Long, Long, Boolean>> loop = stream1
			.join(stream2)
			.where(new KeySelector<Tuple2<Long,Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f1;
				}
			})
			.equalTo(new KeySelector<Tuple2<Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f0;
				}
			})
			.window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE)))
			.apply(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple4<Long, Long, Long, Boolean>>() {
				@Override
				public Tuple4<Long, Long, Long, Boolean> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
					Tuple4<Long, Long, Long, Boolean> tuple4 = new Tuple4<>(first.f0, Math.min(first.f1, second.f1), second.f1, (first.f1 > second.f1));
					System.out.println(tuple4 + " "+first.f1);
					return tuple4;
				}
			})
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Long, Boolean>>() {

				@Override
				public long extractAscendingTimestamp(Tuple4<Long, Long, Long, Boolean> record) {
					return new Timestamp(System.currentTimeMillis()+1000).getTime();
				}
			})
			;


		// ----- Iteration ends

		SplitStream<Tuple4<Long, Long, Long, Boolean>> split = loop.split(
			new OutputSelector<Tuple4<Long, Long, Long, Boolean>>() {
				@Override
				public Iterable<String> select(Tuple4<Long, Long, Long, Boolean> value) {
					List<String> output = new ArrayList<>();
					output.add("iterate");
					return output;
				}
			}
		);


		it.closeWith(split.select("iterate"));
		// close iteration
		split.select("output").print();

		env.execute("Streaming Connected components");
	}


	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
//			out.collect(new Tuple2<>(edge.f0, edge.f0));
//			out.collect(new Tuple2<>(edge.f1, edge.f1));
		}
	}

	public static final class ExtractVertices<T> implements MapFunction<Tuple2<T,T>, Tuple3<T, T, Long>> {

		@Override
		public Tuple3<T,T,Long> map(Tuple2<T,T> t) throws Exception {
			return new Tuple3<T,T,Long>(t.f0, t.f0, 0L);
		}
	}

	private static class EdgeSource implements SourceFunction<Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private long source = 0;
		private long dest = 1;

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			while (isRunning) {
				if (source % 20 == 0) {
					source++;
					dest++;
				}
				if (source >= 39){
					isRunning = false;
				}

				ctx.collect(new Tuple2<Long, Long>(source++, dest++));

//				Thread.sleep(50L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
