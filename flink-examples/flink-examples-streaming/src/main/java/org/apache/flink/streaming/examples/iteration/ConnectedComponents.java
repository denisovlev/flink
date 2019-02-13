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

public class ConnectedComponents {
	public static void main(String args[]) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setBufferTimeout(1).setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// edges reference [ (1,2), (2,1), (2,3), (3,2) ...]
		DataStream<Tuple2<Long, Long>> inputEdge = env.addSource(new EdgeSource()).flatMap(new UndirectEdge());

		// initial vertices values: pair for id with 0 iteration [(1,1,0), (2,2,0), ..]
		DataStream<Tuple3<Long, Long, Long>> verticesWithInitialId = inputEdge.map(new ExtractVertices<>());

		// define iteration
		IterativeStream<Tuple3<Long,Long, Long>> it = verticesWithInitialId.iterate(1000);

		// ----- Iteration start
		DataStream<Tuple3<Long, Long,Long>> changes =
			it
				// assign timestamp
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Long, Long>>() {

					@Override
					public long extractAscendingTimestamp(Tuple3<Long, Long, Long> record) {
						return new Timestamp(System.currentTimeMillis()).getTime();
					}
				})
				// join current result with edges list
				.join(inputEdge).where(new KeySelector<Tuple3<Long, Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple3<Long, Long, Long> value) throws Exception {
					return value.f0;
				}
			})
				// join
				.equalTo(new KeySelector<Tuple2<Long, Long>, Long>() {
					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				})
				// windowing by tumbling window (?)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))

				// apply function pf the join
				.apply(
					new JoinFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
						@Override
						public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple2<Long, Long> second) throws Exception {
							return new Tuple3<>(first.f0, second.f1, first.f2 + 1);
						}
					}
				)
				.keyBy(0) // key by vertices id
				.minBy(1); // minimum by the second element, e.g result of join
		// ----- Iteration ends


		// close iteration
		it.closeWith(changes);
		changes.print();

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
			out.collect(new Tuple2<>(edge.f0, edge.f0));
			out.collect(new Tuple2<>(edge.f1, edge.f1));
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

				Thread.sleep(50L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
