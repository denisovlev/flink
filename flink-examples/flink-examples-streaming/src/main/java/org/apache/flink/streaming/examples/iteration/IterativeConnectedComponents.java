package org.apache.flink.streaming.examples.iteration;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
//import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

public class IterativeConnectedComponents {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectedComponents.class);


	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 4;
	private static String inputFile = "";

	public static void main(String args[]) throws Exception{
		inputFile = args[0];
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
		public final int label;
		public final int vid;

		public Label(int label, int vid) {
			this.label = label;
			this.vid = vid;
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
		env.setParallelism(4);
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
			env.fromCollection(edges)
			.flatMap(new RichFlatMapFunction<Edge, Either<Edge, EOS>>() {

				private transient Collector<Either<Edge, EOS>> out;
				private transient HashSet<Integer> helper;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					helper = new HashSet<>();
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
			env.fromCollection(labels)
			.flatMap(new RichFlatMapFunction<Label, Either<Label, EOS>>() {

				private transient Collector<Either<Label, EOS>> out;
				private transient HashSet<Integer> helper;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					helper = new HashSet<>();
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
					helper.add(value.vid);
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
			.iterate(1000);

		DataStream<Either<Label, EOS>> nextStep = labelsIt
			.keyBy(new KeySelector<Either<Label, EOS>, Integer>() {
				@Override
				public Integer getKey(Either<Label, EOS> value) throws Exception {
					return value.isLeft() ? value.left().label : value.right().u;
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
				private transient ValueState<Integer> minLabel;
				private transient ValueState<Boolean> hasEOF1;
				private transient ValueState<Boolean> hasEOF2;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					outVertex = getRuntimeContext().getListState(new ListStateDescriptor<>("out-vertex", Integer.class));
					minLabel = getRuntimeContext().getState(new ValueStateDescriptor<>("minLabel", Integer.class));
					hasEOF1 = getRuntimeContext().getState(new ValueStateDescriptor<>("eof1", Boolean.class));
					hasEOF2 = getRuntimeContext().getState(new ValueStateDescriptor<>("eof2", Boolean.class));
				}

				@Override
				public void processElement1(Either<Label, EOS> in, Context ctx, Collector<Either<Label, EOS>> out) throws Exception {
					if (in.isLeft()) {
						Integer v;
						if ((v = minLabel.value()) == null) {
							minLabel.update(in.left().vid);

						} else if (v > in.left().vid) {
							minLabel.update(in.left().vid);
						}
					} else {
						hasEOF1.update(true);
						Integer minVal;
						if (hasEOF2.value() != null && hasEOF2.value() && (minVal = minLabel.value()) != null) {
							for (int vertex : outVertex.get()) {
								out.collect(Either.Left(new Label(vertex, minVal)));
							}

							for (int vertex : outVertex.get()) {
								out.collect(Either.Right(new EOS(vertex)));
							}
//							out.collect(Either.Right(new EOS(minVal)));
							hasEOF1.update(false);
						}
					}

					System.out.println("++++++++++++++++++");
					System.out.println(minLabel.value());
					for(Integer i: outVertex.get()){
						System.out.print(i + ", " );
					}
					System.out.println();
					System.out.println("----------------");
				}

				@Override
				public void processElement2(Either<Edge, EOS> in, Context ctx, Collector<Either<Label, EOS>> out) throws Exception {
					if (in.isLeft()) {
						outVertex.add(in.left().u);
					} else {
						hasEOF2.update(true);
						Integer minVal;
						if (hasEOF1.value() != null && hasEOF1.value() && (minVal = minLabel.value()) != null) {
							for (int vertex : outVertex.get()) {
								out.collect(Either.Left(new Label(vertex, minVal)));
							}
							for (int vertex : outVertex.get()) {
								out.collect(Either.Right(new EOS(vertex)));
							}
							hasEOF1.update(false);
						}
					}
				}
			});

		labelsIt
			.closeWith(nextStep)
			.addSink(new SinkFunction<Either<Label, EOS>>() {
				@Override
				public void invoke(Either<Label, EOS> value) throws Exception {
					if (value.isLeft()) {
						LOG.info("Got Out {} <-> {}", value.left().label, value.left().vid);
					} else {
						LOG.info("Got EOS {}", value.right().u);
					}
				}
			});

		env.execute();

	}
}
