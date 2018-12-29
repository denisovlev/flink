package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ConnectedComponents {
	public static void main(String args[]) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setBufferTimeout(1).setParallelism(1);

		DataStream<Tuple2<Long, Long>> inputEdge = env.addSource(new EdgeSource()).flatMap(new UndirectEdge());
		DataStream<Tuple3<Long, Long, Long>> verticesWithInitialId = inputEdge.map(new ExtractVertices<>());

//		inputEdge.print();
//		verticesWithInitialId.print();

		IterativeStream<Tuple3<Long,Long, Long>> it = verticesWithInitialId.iterate(2000);
		DataStream<Tuple3<Long, Long,Long>> changes = null;
		changes =
			it.join(inputEdge).where(new KeySelector<Tuple3<Long, Long, Long>, Long>() {
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

			.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
 			.apply(
				new JoinFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
					@Override
					public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple2<Long, Long> second) throws Exception {
						return new Tuple3<>(first.f1, second.f1, first.f2 + 1);
					}
				}
			)
			.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
			.minBy(1);

		it.closeWith(changes.filter(new FilterFunction<Tuple3<Long, Long, Long>>() {
			@Override
			public boolean filter(Tuple3<Long, Long, Long> value) throws Exception {
				return true;
			}
		}));
		changes.print();

		DataStream<Tuple3<Long, Long,Long>> out = changes.filter(new FilterFunction<Tuple3<Long, Long, Long>>() {
			@Override
			public boolean filter(Tuple3<Long, Long, Long> value) throws Exception {
				return false;
			}
		});

//		out.print();


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
