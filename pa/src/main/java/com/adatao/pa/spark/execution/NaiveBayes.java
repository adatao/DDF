package com.adatao.pa.spark.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ML.NaiveBayesModel;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import adatao.ML.types.TJsonSerializable;
import adatao.ML.types.TJsonSerializable$class;

@SuppressWarnings("serial")
public class NaiveBayes implements IExecutor, Serializable {
	private String dataContainerID;
	private int[] xCols;
	private int yCol;
	
	public static Logger LOG = LoggerFactory.getLogger(NaiveBayes.class);
	
	public NaiveBayes(String dataContainerID, int[] xCols, int yCol) {
		this.dataContainerID = dataContainerID;
		this.xCols = xCols.clone();
		this.yCol = yCol;
	}

	@SuppressWarnings("serial")
	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataManager.DataContainer df = sparkThread.getDataManager().get(dataContainerID);
		
		// convert Object[] RDD into Tuple2<Double, double[]> RDD where:
		// 1. the first parameter of type Double is the class value
		// 2. the second parameter of type double[] is the feature values
		JavaPairRDD<Double, double[]> pairs = df.getRDD().mapPartitions(new PairFlatMapFunction<Iterator<Object[]>, Double, double[]>() {
			@Override
			public Iterable<Tuple2<Double, double[]>> call(Iterator<Object[]> rows) throws Exception {
				ArrayList<Tuple2<Double, double[]>> instances = Lists.newArrayList();
				while (rows.hasNext()) {
					// ignore a row that contains a null element					
					Object[] objs = rows.next();
					boolean ok = true;
					for (int i = 0; i < xCols.length; i++) {
						int col = xCols[i];
						if (objs[col] == null || (objs[col].equals(""))) {
							ok = false;
							break;
						}
					}
					if (objs[yCol] == null || objs[yCol].equals(""))
						ok = false;
					if (!ok) continue;
					double[] features = new double[xCols.length];
					for (int i = 0; i < xCols.length; i++) {
						if (objs[xCols[i]] instanceof Integer)
							features[i] = ((Integer) objs[xCols[i]]).doubleValue();
						else
							features[i] = ((Double) objs[xCols[i]]).doubleValue();
					}
					Double target;
					if (objs[yCol] instanceof Integer) 
						target = (Double) ((Integer) objs[yCol]).doubleValue();
					else 
						target = (Double) ((Double) objs[yCol]).doubleValue();
					instances.add(new Tuple2<Double, double[]>(target, features));
				}
				return instances;
			}
		});

		// initialize Naive Bayes Model
		int numClasses = (int) pairs.keys().distinct().count();
		int numFeatures = xCols.length;
		System.out.println("numClasses: " + numClasses + " numFeatures: " + numFeatures);
		NaiveBayesModel model = new NaiveBayesModel(numClasses, numFeatures, xCols, yCol);

		// compute class prior
		Map<Double, Object> mapKeys = pairs.countByKey();
		long countAll = pairs.count();
		List<Double> keys = pairs.keys().distinct().collect();
		for (Double key : keys) {
			model.setPrior(key.intValue(), ((Double) mapKeys.get(key)).longValue() / countAll);
		}

		// compute feature distribution
		Function2<double[], double[], double[]> sumArrayFunction = new Function2<double[], double[], double[]>(){
			@Override
			public double[] call(double[] t1, double[] t2) throws Exception {
				for (int i = 0; i < t1.length; i++) {
					t1[i] += t2[i];
				}
				return t1;
			}
		};
		
		// compute sum 
		JavaPairRDD<Double, double[]> sumRDD = pairs.reduceByKey(sumArrayFunction);
		
		// compute sum of squares
		JavaPairRDD<Double, double[]> sumSquaresRDD = pairs.mapPartitions(
				new PairFlatMapFunction<Iterator<Tuple2<Double, double[]>>, Double, double[]>() {
			@Override
			public Iterable<Tuple2<Double, double[]>> call(Iterator<Tuple2<Double, double[]>> t) throws Exception {
				List<Tuple2<Double, double[]>> list = Lists.newArrayList();
				while (t.hasNext()) {
					Tuple2<Double, double[]> tup = t.next();
					double[] x = tup._2;
					for (int i = 0; i < x.length; i++) 
						x[i] *= x[i];
					list.add(new Tuple2<Double, double[]>(tup._1, x));
				}
				return list;
			}		
		}).reduceByKey(sumArrayFunction);
		
		Map<Double, double[]> s = sumRDD.collectAsMap();
		Map<Double, double[]> ss = sumSquaresRDD.collectAsMap();
		
		for (Double key : keys) {
			long n = ((Long) mapKeys.get(key)).longValue(); // n is the number of samples with class = key
			for (int i = 0; i < numFeatures; i++) {
				List<Double> values = new ArrayList<Double>();
				double sum = s.get(key)[i];
				double ssum = ss.get(key)[i];

				// sample correction
				double mean = (sum + 1) / (n + numClasses);
				double var = (ssum+1) / (n+numClasses) - mean*mean;
				
				values.add(mean); // add mean
				values.add(var); // add variance
				model.setFeatureDistribution(key.intValue(), i, values);
			}
		}
						
		String modelID = sparkThread.getDataManager().putObject(model);
		NaiveBayesResult result = new NaiveBayesResult(numClasses, numFeatures).setModelId(modelID);
		model.printAll();
		return result;
	}
	
	public static class NaiveBayesResult extends SuccessResult implements TJsonSerializable {
		private String modelID;
		private String clazz;
		private int numClasses;
		private int numFeatures;
		
		public NaiveBayesResult() {
		}
		
		public NaiveBayesResult(int numClasses, int numFeatures) {
			this.numClasses = numClasses;
			this.numFeatures = numFeatures;
		}
		
		public NaiveBayesResult setModelId(String id) {
			this.modelID = id;
			return this;
		}
		
		public String getModelID() {
			return this.modelID;
		}
		
		public int getNumClasses() {
			return this.numClasses;
		}
		
		public int getNumFeatures() {
			return this.numFeatures;
		}

		@Override
		public String clazz() {
			return clazz;
		}

		public void adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
			clazz = Aclass;
		}

		public TJsonSerializable fromJson(String jsonString) {
			return TJsonSerializable$class.fromJson(this, jsonString);
		}
	}
}
