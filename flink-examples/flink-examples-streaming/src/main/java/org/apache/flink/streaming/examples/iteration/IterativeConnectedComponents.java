package org.apache.flink.streaming.examples.iteration;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
//import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class IterativeConnectedComponents {

	private static final Logger LOG = LoggerFactory.getLogger(IterativeConnectedComponents.class);


	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 4;
	private static String inputFile = "";
	private static String outputFile = "";

	public static void main(String args[]) throws Exception{
		inputFile = args[0];
		outputFile = args[1];
		new IterativeConnectedComponents().runCC();
	}
//	private static TestingCluster cluster;

//	@ClassRule
//	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	//	@BeforeClass
	public static void setup() throws Exception {
		// detect parameter change

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
		config.setInteger(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), 2048);

		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setInteger(WebOptions.PORT, 8081);

//		cluster = new TestingCluster(config);
//		cluster.start();

	}

	//	@AfterClass
	public static void shutDownExistingCluster() {
//		if (cluster != null) {
//			cluster.stop();
//		}
	}

	public static class Edge implements Serializable {

		public final int u;

		public final int v;

		public Edge(int u, int v) {
			this.u = u;
			this.v = v;
		}

	}

	public static class Label implements Serializable {
		public final int vid;
		public final int minLabel;

		public Label(int vid, int minLabel) {
			this.vid = vid;
			this.minLabel = minLabel;
		}
	}

	public static class EOS implements Serializable {

		private final int u;

		public EOS(int u) {
			this.u = u;
		}
	}

	public static final int[] VERTICES  = new int[] {
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

	public static final Integer[][] EDGES = new Integer[][] {
		new Integer[]{1, 2},
		new Integer[]{2, 3},
		new Integer[]{2, 4},
		new Integer[]{3, 5},
		new Integer[]{6, 7},
		new Integer[]{8, 9},
		new Integer[]{8, 10},
		new Integer[]{5, 11},
		new Integer[]{11, 12},
		new Integer[]{10, 13},
		new Integer[]{9, 14},
		new Integer[]{13, 14},
		new Integer[]{1, 15},
		new Integer[]{16, 1}
	};

	private static DataStream<Edge> getEdgesDataSet(StreamExecutionEnvironment env) {

		return env.readTextFile(inputFile )
			.flatMap(new FlatMapFunction<String, Edge>() {
				@Override
				public void flatMap(String value, Collector<Edge> out) throws Exception {
					String[] s = value.split("\\s+");
					int a = Integer.parseInt(s[0]);
					int b = Integer.parseInt(s[1]);
					out.collect(new Edge(a, b));
					out.collect(new Edge(b, a));
				}
			});
	}


	//	@Test
	public void runCC() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(1);
		ArrayList<Edge> edges = new ArrayList<>();

		for (Integer[] o : EDGES) {
			edges.add(new Edge(o[0], o[1]));
			edges.add(new Edge(o[1], o[0]));
		}

		ArrayList<Label> labels = new ArrayList<>();

		for (int v : VERTICES) {
			labels.add(new Label(v, v));
		}

		DataStream<Edge> edgesStream = getEdgesDataSet(env);
		DataStream<Label> labelDataStream =
			edgesStream.map(new MapFunction<Edge, Label>() {
				@Override
				public Label map(Edge value) throws Exception {
					return new Label(value.u, value.u);
				}
			});

		DataStream<Either<Edge, EOS>> edgeStream =
			edgesStream
//			env.fromCollection(edges)
			.flatMap(new RichFlatMapFunction<Edge, Either<Edge, EOS>>() {

				private transient Collector<Either<Edge, EOS>> out;
				private transient List<Integer> helper;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					helper = new ArrayList<>();
					out = null;
				}

				@Override
				public void close() throws Exception {
					for (Integer u : helper) {
						out.collect(Either.Right(new EOS(u)));
					}
				}

				@Override
				public void flatMap(Edge value, Collector<Either<Edge, EOS>> out) throws Exception {
					if (this.out == null) {
						this.out = out;
					}
					out.collect(Either.Left(value));
					helper.add(value.u);
				}
			});

		DataStream<Either<Label, EOS>> labelStream =
//			env.fromCollection(labels)
			labelDataStream
			.flatMap(new RichFlatMapFunction<Label, Either<Label, EOS>>() {

				private transient Collector<Either<Label, EOS>> out;
				private transient List<Integer> helper;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					helper = new ArrayList<>();
					out = null;
				}

				@Override
				public void close() throws Exception {
					for (Integer u : helper) {
						out.collect(Either.Right(new EOS(u)));
					}
				}

				@Override
				public void flatMap(Label value, Collector<Either<Label, EOS>> out) throws Exception {
					if (this.out == null) {
						this.out = out;
					}
					out.collect(Either.Left(value));
					helper.add(value.minLabel);
				}
			});

		IterativeStream<Either<Label, EOS>> labelsIt = labelStream
			.forward()
			.map(new MapFunction<Either<Label, EOS>, Either<Label, EOS>>() {
				@Override
				public Either<Label, EOS> map(Either<Label, EOS> in) throws Exception {
					return in;
				}
			})
			.iterate(10000);

		DataStream<Either<Label, EOS>> nextStep = labelsIt
			.keyBy(new KeySelector<Either<Label, EOS>, Integer>() {
				@Override
				public Integer getKey(Either<Label, EOS> value) throws Exception {
					return value.isLeft() ? value.left().vid : value.right().u;
				}
			})
			.connect(edgeStream.keyBy(new KeySelector<Either<Edge, EOS>, Integer>() {
				@Override
				public Integer getKey(Either<Edge, EOS> value) throws Exception {
					return value.isLeft() ? value.left().v : value.right().u;
				}
			}))
			.process(new CoProcessFunction<Either<Label, EOS>, Either<Edge, EOS>, Either<Label, EOS>>() {

				private transient ListState<Integer> outVertex;
				private transient ValueState<Integer> outVertexSeenCnt;
				private transient ValueState<Integer> outVertexEndSeenCnt;
				private transient ValueState<Integer> labelsSeenCnt;
				private transient ValueState<Integer> labelsEndSeenCnt;
				private transient ValueState<Integer> minLabel;
				private transient ValueState<Boolean> hasEOF1;
				private transient ValueState<Boolean> hasEOF2;
				private transient ValueState<Boolean> minChanged;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					outVertex = getRuntimeContext().getListState(new ListStateDescriptor<>("out-vertex", Integer.class));
					outVertexSeenCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("out-vertex-seen", Integer.class));
					outVertexEndSeenCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("out-vertex-end-seen", Integer.class));
					labelsSeenCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("labels-seen", Integer.class));
					labelsEndSeenCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("labels-end-seen", Integer.class));
					minLabel = getRuntimeContext().getState(new ValueStateDescriptor<>("minLabel", Integer.class));
					hasEOF1 = getRuntimeContext().getState(new ValueStateDescriptor<>("eof1", Boolean.class));
					hasEOF2 = getRuntimeContext().getState(new ValueStateDescriptor<>("eof2", Boolean.class));
					minChanged = getRuntimeContext().getState(new ValueStateDescriptor<>("min-changed", Boolean.class));
				}

				private boolean seenAll() throws Exception {

					int labelsSize = labelsSeenCnt.value();
					int labelsSeensize = labelsEndSeenCnt.value();
					int outVertexSize = outVertexSeenCnt.value();
					int outVertexSeenSize = outVertexEndSeenCnt.value();

					return 	labelsSize == labelsSeensize &&
							outVertexSize == outVertexSeenSize;
				}

				private void resetSeenValues() throws IOException {
					labelsSeenCnt.update(0);
					labelsEndSeenCnt.update(0);
				}

				@Override
				public void processElement1(Either<Label, EOS> in, Context ctx, Collector<Either<Label, EOS>> out) throws Exception {
					if (in.isLeft()) {
						incCounter(labelsSeenCnt);
						Integer v;
						if ((v = minLabel.value()) == null) {
							minLabel.update(in.left().minLabel);
							minChanged.update(true);
						} else if (v > in.left().minLabel) {
							minLabel.update(in.left().minLabel);
							minChanged.update(true);
						}
					} else {
						hasEOF1.update(true);
						Integer minVal;
						incCounter(labelsEndSeenCnt);
						if (hasEOF2.value() != null && hasEOF2.value() && (minVal = minLabel.value()) != null && minChangedValue() && seenAll()) {
							for (int vertex : outVertex.get()) {
								out.collect(Either.Left(new Label(vertex, minVal)));
							}

							for (int vertex : outVertex.get()) {
								out.collect(Either.Right(new EOS(vertex)));
							}
							hasEOF1.update(false);
							minChanged.update(false);
							resetSeenValues();
						}
					}
				}

				private void incCounter(ValueState<Integer> cnt) throws IOException {
					if (cnt.value() == null) cnt.update(0);
					cnt.update(cnt.value() + 1);
				}

				private boolean minChangedValue() throws java.io.IOException {
					return minChanged.value() != null && minChanged.value();
//					return true;
				}

				@Override
				public void processElement2(Either<Edge, EOS> in, Context ctx, Collector<Either<Label, EOS>> out) throws Exception {
					if (in.isLeft()) {
						outVertex.add(in.left().u);
						incCounter(outVertexSeenCnt);
					} else {
						incCounter(outVertexEndSeenCnt);
						hasEOF2.update(true);
						Integer minVal;
						if (hasEOF1.value() != null && hasEOF1.value() && (minVal = minLabel.value()) != null && minChangedValue()  && seenAll()) {
							for (int vertex : outVertex.get()) {
								out.collect(Either.Left(new Label(vertex, minVal)));
							}
							for (int vertex : outVertex.get()) {
								out.collect(Either.Right(new EOS(vertex)));
							}
							hasEOF1.update(false);
							minChanged.update(false);
							resetSeenValues();
						}
					}
				}
			});

		SplitStream<Either<Label, EOS>> splitStream = nextStep.split((OutputSelector<Either<Label, EOS>>) value -> {
			List<String> output = new ArrayList<>();
			output.add("output");
			output.add("iterate");

			return output;
		});

		labelsIt
			.closeWith(splitStream.select("iterate"));

//		labelsIt.print();
		splitStream.select("output")
			.filter(new FilterFunction<Either<Label, EOS>>() {
				@Override
				public boolean filter(Either<Label, EOS> value) throws Exception {
					return value.isLeft();
				}
			})
			.map(new MapFunction<Either<Label, EOS>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Either<Label, EOS> label) throws Exception {
					return new Tuple2<Integer, Integer>(label.left().vid, label.left().minLabel);
				}
			})
			.writeAsCsv(outputFile, FileSystem.WriteMode.OVERWRITE, "\n", " ")
//			.keyBy(new KeySelector<Either<Label,EOS>, Integer>() {
//				@Override
//				public Integer getKey(Either<Label, EOS> value) throws Exception {
//					return value.left().minLabel;
//				}
//			})
//			.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//			.process(new ProcessWindowFunction<Either<Label,EOS>, Tuple2<Integer, String>, Integer, TimeWindow>() {
//				@Override
//				public void process(Integer key, Context context, Iterable<Either<Label, EOS>> elements, Collector<Tuple2<Integer, String>> out) throws Exception {
//					HashSet<String> set = new HashSet<String>();
//					for(Either<Label, EOS> element: elements){
//						set.add(Integer.toString(element.left().vid));
//					}
////					String s = String.join("|", set);
//					String s = Integer.toString(set.size());
//					out.collect(new Tuple2<>(key, s));
//				}
//
//			})
//			.print()
			;

		env.execute();

	}
}
