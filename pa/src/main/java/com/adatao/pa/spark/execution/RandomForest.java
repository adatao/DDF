/***
 * Copyright 2013, ADATAO INC 
 * @author long@adatau.com
 */
package com.adatao.pa.spark.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import com.adatao.ML.RandomForestModel;
import com.adatao.pa.ML.types.randomforest.data.Bagging;
import com.adatao.pa.ML.types.randomforest.data.Data;
import com.adatao.pa.ML.types.randomforest.data.DataFormat;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.adatao.pa.ML.types.randomforest.node.Node;
import com.adatao.pa.ML.types.randomforest.tree.TreeBuilder;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.common.collect.Lists;
import com.adatao.ML.types.TJsonSerializable;
import com.adatao.ML.types.TJsonSerializable$class;

/***
 * Training executor for Random Forest
 */
@SuppressWarnings("serial")
public class RandomForest implements IExecutor, Serializable {
	private String dataContainerID;
	private int[] xCols;
	private int yCol;
	private int mTry;
	private int numTrees;
	protected long seed;
	private final int bagCount = 5;
	private boolean oobFlag = false;

	public static Logger LOG = LoggerFactory.getLogger(RandomForest.class);

	public RandomForest(String dataContainerID, int[] xCols, int yCol, int mTry, int numTrees, long seed) {
		this.dataContainerID = dataContainerID;
		this.xCols = xCols.clone();
		this.yCol = yCol;
		this.mTry = mTry;
		this.numTrees = numTrees;
		this.seed = seed;
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataManager.DataContainer df = sparkThread.getDataManager().get(dataContainerID);

		JavaRDD<Object[]> rdd = df.getRDD(); // all of the data here
		DataManager.MetaInfo[] metaInfo = df.getMetaInfo();

		/*
		 * 1. We determine which columns to get factor
		 */
		List<Integer> sCols = Lists.newArrayList();
		for (int i = 0; i < xCols.length; i++) {
			LOG.info("Data type of column " + xCols[i] + " is " + metaInfo[xCols[i]].getType());
		}
		LOG.info("Target type of column " + yCol + " is " + metaInfo[yCol].getType());

		for (int i = 0; i < xCols.length; i++) {
			int col = xCols[i];
			if (metaInfo[col].getType().toLowerCase().contains("string") && !metaInfo[col].hasFactor()) {
				sCols.add(col);
			}
		}
		sCols.add(yCol);
		LOG.info("Number of columns to get factors: " + sCols.size());

		int[] stringCols = new int[sCols.size()];
        for (int i = 0; i < stringCols.length; i++) stringCols[i] = sCols.get(i);
		GetMultiFactor getMultiFactor = new GetMultiFactor(dataContainerID, stringCols);
		Tuple2<Integer, Map<String, Integer>>[] tuple2s = getMultiFactor.runImpl(new ExecutionContext(sparkThread));

		// LOGIC: 
		// + If xCols[i] is string/int => CATEGORICAL, else => NUMERICAL
		// + If yCol has only two values => CLASSIFICATION, else REGRESSION
		// isRegression = !metaInfo[yCol].hasFactor()
		if (tuple2s[tuple2s.length-1]._2().size() != 2) 
			metaInfo[yCol].setFactor(null);

		final DataFormat dataFormat = new DataFormat(metaInfo, xCols, yCol);

		// determine mtry
		if (mTry == 0) {
			if (dataFormat.isRegression())
				mTry = (int) Math.ceil((dataFormat.numAttributes() - 1) / 3.0);
			else 
				mTry = (int) Math.ceil(Math.sqrt(dataFormat.numAttributes() - 1));
		}
		dataFormat.printMapValues();

		/*
		 * 2. Filter Object[] containing null elements
		 */
		rdd = rdd.mapPartitions(new FlatMapFunction<Iterator<Object[]>, Object[]>(){
			@Override
			public Iterable<Object[]> call(Iterator<Object[]> arg0) {
				ArrayList<Object[]> objs = Lists.newArrayList();
				while (arg0.hasNext()) {
					Object[] obj = arg0.next();
					boolean ok = true;
					for (int i = 0; i < xCols.length; i++) 
						if (obj[xCols[i]] == null) {
							ok = false;
							break;
						}
					if (ok && obj[yCol] != null) 
						objs.add(obj);
				}
				return objs;
			}	
		});

		/*
		 * 3. Simulate bagging with the following procedure: 
		 * + Repeat bagCount times: random split by 2/3 in size of the original data set (sample with replacement).
		 * + For each of train/test split: train (numTrees/bagCount) trees on the train set, collect and use them to predict the test set
		 * + Hence, for each train set, we generate (numTrees/bagCount)/numOutputPartitions tree for each partition. 
		 */
		final JavaRDD<Object[]> rddO = rdd;
		/*
		 * Random Split
		 */

		DataManager.DataContainer dc = new DataManager.DataFrame(metaInfo, rddO);

		String dcID = sparkThread.getDataManager().add(dc);
		CVKFoldSplit splitter = new CVKFoldSplit(dcID, bagCount, seed);
		// TODO CVKFoldSplit may return 0-size split! Be careful on small data set.
		String[][] splits = splitter.runImpl(new ExecutionContext(sparkThread)); 
		String[] trainSet = new String[bagCount];
		String[] testSet = new String[bagCount];
		for (int i = 0; i < bagCount; i++) {
			trainSet[i] = splits[i][0];
			testSet[i] = splits[i][1];
		}

		List<Node> forest = new ArrayList<Node>();
		long[] cfmatrix = null;

		if (dataFormat.isRegression()) {

		} else {
			if (oobFlag) 
				cfmatrix = new long[4];
		}

		for (int i = 0; i < bagCount; i++) {			
			/*
			 * 4.1 Convert trainRDD Object[] to Instance 
			 */
			JavaRDD<Object[]> trainRDD = sparkThread.getDataManager().get(trainSet[i]).getRDD();

			//LOG.info("train " + i + " size=" + trainRDD.collect().size());
			JavaRDD<Instance> instances = trainRDD.mapPartitions(new FlatMapFunction<Iterator<Object[]>, Instance>() {
				@Override
				public Iterable<Instance> call(Iterator<Object[]> rows)  {
					ArrayList<Instance> instances = new ArrayList<Instance>();
					while (rows.hasNext()) {					
						instances.add(dataFormat.convert(rows.next()));
					}
					return instances;
				}
			});

			int numInputPartitions = instances.splits().size();
			LOG.info("Bag " +  i +  " number of partitions: " + numInputPartitions);
			final int numOutputPartitions = numInputPartitions * 2;
			final Random random = new Random(seed);

			/*
			 * 4.2 Shuffle RDDs with replacement. Generate 2 times more partitions, each partition is of the same size
			 */
			instances = instances.mapPartitions(new PairFlatMapFunction<Iterator<Instance>, Integer, Instance>(){
				@Override
				public Iterable<Tuple2<Integer, Instance>> call(Iterator<Instance> inst) {
					List<Tuple2<Integer, Instance>> subSets = new ArrayList<Tuple2<Integer, Instance>>();

					List<Instance> l = new ArrayList<Instance>(); 
					while (inst.hasNext()) { 
						l.add(inst.next());
					}
					int N = l.size();
					if (N == 0) return subSets;

					long runSeed = random.nextLong();
					Random rnd = new Random(runSeed);

					if(numOutputPartitions != 0) {
						for (int key = 0; key < numOutputPartitions; key++) {
							for (int i = 0; i < N/numOutputPartitions + 1; i++) {
								Instance value = l.get(rnd.nextInt(N)); // with replacement
								subSets.add(new Tuple2<Integer, Instance>(key, value));  // throw out the tuples of (key=id of new partition, value) 
							}
						}
					}
					else {
						LOG.error(">>>>> numOutputPartitions is ZERO " + numOutputPartitions);
					}
					return subSets;
				}
			})
			.groupByKey(numOutputPartitions) // group all by key into new partitions
			.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, List<Instance>>>, Instance>(){ // aggregate JavaPairRDD<Integer, List<Instance>> into JavaRDD<Instance>
				@Override
				public Iterable<Instance> call(Iterator<Tuple2<Integer, List<Instance>>> arg0)  {
					List<Instance> res = new ArrayList<Instance>();
					while (arg0.hasNext()) {
						res.addAll(arg0.next()._2);
					}
					return res;
				}
			});

			/*
			 * 4.3 Build trees on the Instances on each partition 
			 */
			JavaRDD<Node> nodes = instances.mapPartitions(new FlatMapFunction<Iterator<Instance>, Node>() {
				@Override
				public Iterable<Node> call(Iterator<Instance> inst) throws Exception {
					List<Instance> pInstances = new ArrayList<Instance>();
					while (inst.hasNext()) {
						pInstances.add(inst.next());
					}
					Data data = new Data(dataFormat, pInstances);
					data.sort(); // make data order deterministic

					LOG.info("data size = " + data.size());

					List<Node> trees = new ArrayList<Node>();
					// Bagging
					//					Bagging bagging = new Bagging(data, random.nextLong());
					//					Map<Integer, List<Integer>> votes = new HashMap<Integer, List<Integer>>();

					// to make sure exactly numTrees are produced, we create 
					// (numTrees/nbPartitions + 1) trees on each partition and discard the extra ones 
					if(numOutputPartitions != 0) {
						int nRuns = (numTrees % numOutputPartitions == 0) ? (numTrees / numOutputPartitions) : (numTrees / numOutputPartitions) + 1;
						for (int i = 0; i < nRuns; i++) {
							//Data[] subData = bagging.sampleWithPlacement();

							TreeBuilder treeBuilder = new TreeBuilder(data, random.nextLong());
							if (mTry > 0)
								treeBuilder.setMVars(mTry);
							Node tree = treeBuilder.build();
							trees.add(tree);
						}
					}
					else 
						LOG.error(">>>>>> numOutputPartitions = 0");
					return trees;
				}
			});

			/*
			 * 4.4 Collect trees and predict out-of-bag error if requested
			 */
			List<Node> treesInBag = nodes.collect();

			if (oobFlag) {
				RandomForestModel model = new RandomForestModel(dataFormat, treesInBag, this.seed);
				String modelID = sparkThread.getDataManager().putObject(model);

				/*
				 * 4.4.1 Convert testRDD Object[] to Instance 
				 */
				JavaRDD<Object[]> testRDD = sparkThread.getDataManager().get(testSet[i]).getRDD();
				instances = testRDD.mapPartitions(new FlatMapFunction<Iterator<Object[]>, Instance>() {
					@Override
					public Iterable<Instance> call(Iterator<Object[]> rows) throws Exception {
						ArrayList<Instance> instances = new ArrayList<Instance>();
						while (rows.hasNext()) {					
							instances.add(dataFormat.convert(rows.next()));
						}
						return instances;
					}
				});

				//LOG.info("test " + i + " size=" + testRDD.collect().size());
				/*
				 * 4.4.2 fetch testSet to get the confusion matrix
				 */
				if (dataFormat.isRegression()) {

				} else {
					BinaryConfusionMatrix bcm = new BinaryConfusionMatrix(testSet[i], modelID, xCols, yCol, 0.5);
					BinaryConfusionMatrixResult cfm = bcm.runImpl(new ExecutionContext(sparkThread));
					cfmatrix[0] += cfm.truePos();
					cfmatrix[1] += cfm.falsePos();
					cfmatrix[2] += cfm.falseNeg();
					cfmatrix[3] += cfm.trueNeg();
				}
			}

			// put these trees into the forest
			forest.addAll(treesInBag);
		}

		// truncate the list of nodes into numTrees
		forest = new ArrayList<Node>(forest.subList(0, numTrees));

		RandomForestModel model = new RandomForestModel(dataFormat, forest, seed);
		String modelID = sparkThread.getDataManager().putObject(model);
		RandomForestResult result = new RandomForestResult(modelID, mTry, forest.size(), seed);

		if (oobFlag)
			result.cfm = cfmatrix;

		if (dataFormat.isRegression())
			result.type = "Regression";
		else
			result.type = "Classification";

		return result;
	}

	@Override
	public String toString() {
		return String.format("RandomForest[dataContainerID = %s, xCols=%s, yCol=%d, mTry=%d, numTrees=%d, seed=%d]",
				dataContainerID, xCols, yCol, mTry, numTrees, seed);
	}

	public static class RandomForestResult extends SuccessResult implements TJsonSerializable {
		public String modelID;
		public String clazz;
		public int mtry;
		public int numTrees;
		public long seed;
		public String type; // = "regression" or "classification" or "unsupervised"
		public long[] cfm;  // a confusion matrix if classification, == null if regression 
		// Double mse; // Mean of squared residuals if regression, == null if classification
		// Double r2; // R2 Score

		public RandomForestResult() {
		}

		public RandomForestResult(String modelID, int mtry, int ntree, long seed) {
			this.modelID = modelID;
			this.mtry = mtry;
			this.numTrees = ntree;
			this.seed = seed;
		}

		public String clazz() {
			return clazz;
		}

		public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
			clazz = Aclass;
		}

		public TJsonSerializable fromJson(String jsonString) {
			return TJsonSerializable$class.fromJson(this, jsonString);
		}
	}
}
