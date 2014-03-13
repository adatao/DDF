package com.adatao.pa.spark.execution;
///*
// *  Copyright (C) 2013 Adatao, Inc.
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//
//package adatao.bigr.spark.execution;
//
//import java.io.Serializable;
//
//import org.apache.hadoop.hive.metastore.api.FieldSchema;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.gson.GsonBuilder;
//
//import scala.Tuple2;
//import shark.SharkEnv;
//import shark.api.JavaSharkContext;
//import shark.api.JavaTableRDD;
//import shark.api.Row;
//import spark.api.java.JavaPairRDD;
//import spark.api.java.JavaRDD;
//import spark.api.java.JavaSparkContext;
//import spark.api.java.function.PairFunction;
//import spark.api.java.function.Function;
//import spark.api.java.function.Function2;
//import spark.broadcast.Broadcast;
//import adatao.bigr.spark.DataManager;
//import adatao.bigr.spark.SparkThread;
//import adatao.bigr.spark.DataManager.DataContainer;
//import adatao.bigr.spark.DataManager.SharkDataFrame;
//import adatao.bigr.spark.types.ExecutorResult;
//import adatao.bigr.spark.types.IExecutor;
//import adatao.bigr.spark.types.SuccessResult;
//
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//
//@SuppressWarnings("serial")
//public class KmeansJava implements IExecutor, Serializable{
//	private String dataContainerID;
//	private int[] xCols;
//	private int numIterations;
//	private int K;
//	private boolean doCheckData = true; // need to do filter/processing to throw out bad data
//	
//	public static Logger LOG = LoggerFactory.getLogger(KmeansJava.class);
//	
//	static public class KmeansResult extends SuccessResult {
//		
//		//A vector of total within-cluster sum of squared distance over iterations
//		public double[] totalWithins;
//		
//		//number of points in each cluster
//		public int[] pointsPerCluster;
//		
//		//group point per clusterID
//		//Should write this to file
//		//Give user option to get this?
//		//JavaPairRDD<Integer, List<Vector>> pointsGroup ;
//		
//		//List of centroids
//		List<double[]> centroids;
//		
//		public KmeansResult(double[] totWithins
//				,int[] pointsPerCluster
//				,List<double[]> centroids){
//			this.totalWithins = totWithins;
//			this.pointsPerCluster = pointsPerCluster;
//			//this.pointsGroup = pointsGroup;
//			this.centroids = centroids;
//		}
//	}
//	
//	static public class DataPoint implements Serializable {
//
//		double[] x; // coordinates
//		// number of member points, a trick for counting a cluster's members and calculating the centroid coordinates
//		// initially, each point is a centroid of a cluster consisting of only one member, and numMembers = 1;
//		int numMembers;
//		
//		//distance to its closest centroids
//		// initially, each point is a centroid of its own and distance = 0;
//		double distance;
//
//		public DataPoint(double[] x, int numMembers, double distance) {
//			this.x = x;
//			this.numMembers = numMembers;
//			this.distance = distance;
//		}
//	}
//	
//	static public class ParsePoint extends Function<Object[], DataPoint> {
//
//		private int[] base0_XCols; // indices of x values
//		private boolean doCheckData;
//	
//		public ParsePoint(int[] xCols, boolean doCheckData) {
//			this.base0_XCols = new int[xCols.length];
//			for (int i = 0; i < xCols.length; i++) {
//				this.base0_XCols[i] = xCols[i];
//			}
//	
//			this.doCheckData = doCheckData;
//		}
//	
//		@Override
//		public DataPoint call(Object[] dataArray) throws Exception {
//			DataPoint result = null;
//	
//			if (dataArray.length > 0) {
//				try {
//					int dim = this.base0_XCols.length;
//					double[] x = new double[dim];
//					int i = 0;
//	
//					if (this.doCheckData) {
//						// Need to convert/be careful with data parsing
//	
//						for (int xCol : this.base0_XCols) {
//							if (!(dataArray[xCol] instanceof Integer) && !(dataArray[xCol] instanceof Double)) {
//								// disregard current row, na.action=null
//								//System.err.println("detect NaN: dataArray[xCol] = " + dataArray[xCol]);
//								return null;
//							}
//	
//							x[i++] = (Double) dataArray[xCol];
//						}
//	
//						result = new DataPoint(x, 1, 0.0);
//					}
//					else {
//						// Data is already in doubles
//						for (int xCol : this.base0_XCols) {
//							x[i++] = (Double) dataArray[xCol];
//						}
//						result = new DataPoint(x, 1, 0.0);
//					}
//	
//					// throw new Exception("Bad this.xCols[0]" + dataArray[this.xCols[0]].getClass());
//				}
//				catch (Exception e) {
//					LOG.error("Exception while parsing data object: this.xCols.length=" + this.base0_XCols.length, e);
//				}
//			}
//	
//			return result; // disregard this data point as invalid
//		}
//	}
//	
//	static public class SharkParsePoint extends Function<Row, DataPoint> {
//		private int[] base0_XCols; // indices of x values
//		private DataManager.MetaInfo[] metaInfo;
//
//		public SharkParsePoint(int[] xCols, DataManager.MetaInfo[] metaInfo) {
//			this.base0_XCols = new int[xCols.length];
//			for (int i = 0; i < xCols.length; i++) {
//				this.base0_XCols[i] = xCols[i];
//			}
//			this.metaInfo = metaInfo;
//		}
//
//		@Override
//		public DataPoint call(Row row) throws Exception {
//			if (row == null) {
//				return null;
//			}
//			// NB: does not handle all of Hive's numeric type for now
//			int dim = this.base0_XCols.length;
//			double[] x = new double[dim];
//			Integer x_int;
//			Double x_double;
//			for (int i = 0; i < dim; i++) {
//				String type = metaInfo[i].getType();
//				if (type.equals("int")) {
//					x_int = row.getInt(base0_XCols[i]);
//					if (x_int == null) {
//						return null;
//					}
//					x[i] = x_int;
//				} else if (type.equals("double")) {
//					x_double = row.getDouble(base0_XCols[i]);
//					if (x_double == null) {
//						return null;
//					}
//					x[i] = x_double;
//				} else {
//					return null;
//				}
//			}
//			return new DataPoint(x, 1, 0.0);
//		}
//	}
//	
//	public static KmeansResult train(JavaRDD<DataPoint> points,int numIterations, int K) {
//	  points = points.filter(
//        new Function<DataPoint, Boolean>() {
//          public Boolean call(DataPoint p) {
//            if(p!=null) {
//              for(int i=0; i< p.x.length; i++) {
//                if(Double.isNaN(p.x[i])) return false;
//              }
//              return true;
//            }
//            else {
//              return false;
//            }
//          }
//        }
//        );
//	  points.cache();
//	  
//	  //initialize centroids at random
//		final List<DataPoint> centroids = points.takeSample(false,K,42);
//		long numSamples = centroids.size();
//		if (numSamples < K)
//		  throw new RuntimeException("Not enough data points. May be it is the result of null filtering");
//		
//		JavaPairRDD<Integer, DataPoint> closestPoints;
//		double[] total_withinss = new double[numIterations];
//		int iter = 0;
//		
//		closestPoints = points.map(
//				new PairFunction<DataPoint, Integer, DataPoint>(){
//					@Override
//					public Tuple2<Integer,DataPoint> call(DataPoint vector) throws Exception{
//
//						//closest cluster/centroid
//						int clusterID = 0;
//						double closest = Double.POSITIVE_INFINITY;
//						for(int i =0; i< centroids.size(); i++){
//							DataPoint c = centroids.get(i);
//							int dim = c.x.length;
//							// ignore the 1sh element
//							double dist = 0;
//							for(int j = 0; j < dim; j++) {
//								dist += Math.pow(c.x[j] - vector.x[j], 2);
//							}
//							if(dist < closest){
//								closest = dist;
//								clusterID = i;
//							}
//						}
//
//						vector.distance = closest;
//
//						return new Tuple2<Integer, DataPoint>(
//								clusterID, vector);
//					}
//				});
//		
//		do{
//			long startTime   = System.currentTimeMillis();
//			// reducByKey to utilize combiners
//			Map<Integer,DataPoint> newCentroids = closestPoints.reduceByKey(new Function2<DataPoint,DataPoint,DataPoint>(){
//				public DataPoint call(DataPoint p1, DataPoint p2) throws Exception{
//					//in-place adding, reduce GC
//					double[] px = new double[p1.x.length];
//					int py = 0;
//					double pdistance = 0;
//					for(int i = 0; i < p1.x.length; i++) {
//						// running average, nice optimization from @Khang, this saves us a loop over centroids to calculate theirs coordinates
//						px[i] = (p1.x[i]*p1.numMembers + p2.x[i]*p2.numMembers) / (p1.numMembers + p2.numMembers);
//					}
//					py = p1.numMembers + p2.numMembers;
//					
//					//sum of distance on the same key
//					pdistance = p1.distance + p2.distance;
//					return new DataPoint(px, py, pdistance);
//				}
//			}).collectAsMap();
//			
//			
//			
//			//set new centroids
//			for(Map.Entry<Integer, DataPoint> t: newCentroids.entrySet()){
//				centroids.set(t.getKey(), t.getValue());
//			}
//			
//			//calculate total withinss
//			total_withinss[iter] = 0.0;
//			for(int i = 0; i<K; i++){
//				total_withinss[iter] += centroids.get(i).distance;
//			}
//			
//			// allocate each vector to closest centroid
//			closestPoints = points.map(
//					new PairFunction<DataPoint, Integer, DataPoint>(){
//						@Override
//						public Tuple2<Integer,DataPoint> call(DataPoint vector) throws Exception{
//
//							//closest cluster/centroid
//							int clusterID = 0;
//							double closest = Double.POSITIVE_INFINITY;
//							for(int i =0; i< centroids.size(); i++){
//								DataPoint c = centroids.get(i);
//								int dim = c.x.length;
//								// ignore the 1sh element
//								double dist = 0;
//								for(int j = 0; j < dim; j++) {
//									dist += Math.pow(c.x[j] - vector.x[j], 2);
//								}
//								if(dist < closest){
//									closest = dist;
//									clusterID = i;
//								}
//							}
//
//							vector.distance = closest;
//
//							return new Tuple2<Integer, DataPoint>(
//									clusterID, vector);
//						}
//					});
//			
//			
//			LOG.info("total withinss:" + total_withinss);
//			iter += 1;
//			System.out.println("Iteration " + iter + " : " + (System.currentTimeMillis()-startTime) + " ms");
//		}while(iter < numIterations);
//		
//		
//		// No need an additional map here, cause we have each cluster's number of points already
//		int[] pointsPerCluster = new int[centroids.size()];
//		List<double[]> centroid_coords = new ArrayList<double[]>(K);
//		for(int i = 0; i < K; i++) {
//			pointsPerCluster[i] = centroids.get(i).numMembers;
//			centroid_coords.add(centroids.get(i).x);
//		}
//		
//		return new KmeansResult(total_withinss,pointsPerCluster, centroid_coords);
//	}
//	
//	@Override
//	public String toString(){
//		return String
//				.format("KmeansJava [dataContainerID = %s,  xCols=%s, K = %s" +
//						", numIterations = %s]", dataContainerID, xCols, K, numIterations);
//						
//	}
//	
//	@Override
//	public ExecutorResult run(SparkThread sparkThread){
//		DataManager.DataContainer df = sparkThread.getDataManager().get(dataContainerID);
//		JavaRDD<DataPoint> points = null;
//		if (df.getType() == DataContainer.ContainerType.DataFrame) {
//			points = df.getRDD().map(new ParsePoint(this.xCols, this.doCheckData));
//		} else if (df.getType() == DataContainer.ContainerType.SharkDataFrame) {
//			SharkDataFrame sdf = (SharkDataFrame) df;
//			JavaTableRDD rdd = sdf.getTableRDD();
//
//			DataManager.MetaInfo[] meta = sdf.getMetaInfo();
//
//			points = rdd.map(new SharkParsePoint(xCols, meta));
//		}
//		
//		return train(points, numIterations, K);
//		
//	}
//	
//	public static void main(String[] args){
//		if(args.length < 4){
//			System.out.println("Usage: KmeansJava <file_or_sharktable> <xCols> <numClusters> <numIterations>");
//			System.out.println("Example (local file - local mode): KmeansJava file://Kmeans_small 1,2 10 10");
//			System.out.println("Example (hdfs file - cluster mode): KmeansJava hdfs://Kmeans_small 1,2 10 10");
//			System.out.println("Example (shark table - cluster mode): KmeansJava Kmeans_small 1,2 10 10");
//			System.exit(-1);
//			
//		}
//		
//		String fileURL = args[0];
//		String[] strArray = args[1].split(",");
//		int[] xCols = new int[strArray.length];
//		for(int i = 0; i< strArray.length; i++){
//			xCols[i] = Integer.parseInt(strArray[i]);
//		}
//		
//		int numClusters = Integer.parseInt(args[2]);
//		int numIterations = Integer.parseInt(args[3]);
//		System.out.printf("KmeansJava( %s, data=%s, numClusters=%d, numIterations=%d) \n", Arrays.toString(xCols),
//				fileURL, numClusters, numIterations);
//		
//		JavaRDD<DataPoint> points = null;
//		KmeansResult r = null;
//		
//		String SPARK_HOME = System.getenv("SPARK_HOME");
//		String SPARK_MASTER = System.getenv("SPARK_MASTER");
//		String RSERVER_HOME = System.getenv("RSERVER_HOME");
//		String[] JOB_JAR = {RSERVER_HOME + "/target/bigr_server_2.9.3-0.1.jar"};
//		if (SPARK_MASTER == null || SPARK_HOME == null || RSERVER_HOME == null){
//			LOG.warn("SPARK_MASTER or SPARK_HOME or RSERVER_HOME  not defined");
//			System.exit(1);
//		}
//		
//		if (fileURL.startsWith("hdfs://") || (fileURL.startsWith("file://"))) {
//			if (fileURL.startsWith("hdfs://") && (SPARK_MASTER.equals("local"))){
//				LOG.warn("HDFS files only work with cluster mode, ie SPARK_MASTER begins with \"mesos://\" or \"spark://\"");
//				System.exit(1);
//			}
//			
//			int[] loadCols = new int[xCols.length];
//			System.arraycopy(xCols, 0, loadCols, 0, xCols.length);
//			System.out.println("parsing columns " + Arrays.toString(loadCols));
//
//			// selective load will change the column index
//			// NOTE: these are 1-based!!!
//			for (int i = 0; i < loadCols.length; i++) {
//				xCols[i] = i + 1;
//			}
//				
//			JavaSparkContext sc = new JavaSparkContext(SPARK_MASTER, "KmeansJava", SPARK_HOME, JOB_JAR);
//			//Use LoadTable to get RDD<Object[]>
//			LoadTable lt = new LoadTable().setFileURL(fileURL).setSeparator(" ").setColumns(loadCols).setDoDropNaNs(true);
//			JavaRDD<String> fileRDD = sc.textFile(fileURL);
//			DataManager.MetaInfo[] metaInfo = lt.getMetaInfo(fileRDD);
//			Broadcast<DataManager.MetaInfo[]> broadcastMetaInfo = sc.broadcast(metaInfo);
//			JavaRDD<Object[]> df = lt.getDataFrame(fileRDD, broadcastMetaInfo);
//			
//			points = df.map(new ParsePoint(xCols,true)).cache();
//			r = train(points,  numIterations, numClusters);
//		} else {
//			JavaSharkContext sc = SharkEnv.initWithJavaSharkContext(new JavaSharkContext(SPARK_MASTER, "KMeans",
//					SPARK_HOME, JOB_JAR));
//			if (SPARK_MASTER.startsWith("mesos://")) {
//				sc.sql("set javax.jdo.option.ConnectionURL=jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true");
//				sc.sql("set javax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver");
//				sc.sql("set javax.jdo.option.ConnectionUserName=adatao");
//				sc.sql("set javax.jdo.option.ConnectionPassword=ada!@#");
//				sc.sql("set HADOOP_HOME=/root/ephemeral-hdfs");
//			} else if (SPARK_MASTER.equals("local")) {
//				sc.sql("set shark.data.path=resources/"+fileURL);
//				sc.sql("drop table if exists " + fileURL);
//				sc.sql("create table " + fileURL + "(v1 double, v2 double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n' STORED AS TEXTFILE");
//				sc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.data.path}' INTO TABLE " + fileURL);
//			}
//			
//			JavaTableRDD df = sc.sql2rdd("select * from " + fileURL);
//			List<FieldSchema> fs = df.schema();
//			DataManager.MetaInfo[] meta = new DataManager.MetaInfo[fs.size()];
//			for (int i = 0; i < fs.size(); i++) {
//				meta[i] = new DataManager.MetaInfo(fs.get(i).getName(), fs.get(i).getType());
//			}
//
//			// NB: cols are 1-based
//			points = df.map(new SharkParsePoint(xCols, meta)).cache();
//			r = train(points,  numIterations, numClusters);
//		}
//		
//		String js = new GsonBuilder().serializeSpecialFloatingPointValues().create().toJson(r);
//		System.out.println("JSON result = " + js);
//		
//	}
//			
//
//}
