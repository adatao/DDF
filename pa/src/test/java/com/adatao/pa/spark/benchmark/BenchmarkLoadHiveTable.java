/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark.benchmark;

import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import shark.SharkEnv;
import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;
import shark.api.Row;
import shark.memstore2.ColumnarStruct;
import shark.memstore2.TablePartition;
import shark.memstore2.TablePartitionIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
@Ignore("This is performance benchmark - only for benchmarking")
public class BenchmarkLoadHiveTable {
	public static Logger LOG = LoggerFactory.getLogger(BenchmarkLoadHiveTable.class);

	private static final String SPARK_MASTER = System.getenv("SPARK_MASTER");
	private static final String SPARK_HOME = System.getenv("SPARK_HOME");
	private static final String RSERVER_JAR = System.getenv("RSERVER_JAR");

	/**
	 * Copy data out using buffer copy, null not filtered
	 * Output data is stored as a list of partitioned columns of primitive type
	 */
	static public class TP2DoublePrimitiveArrayMapper extends Function<Row, List<double[]>> {
		int[] columnList;

		public TP2DoublePrimitiveArrayMapper() {
			super();
		}

		public TP2DoublePrimitiveArrayMapper(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
		/**
		 * Copy data out using DoubleBuffer, null not filtered		 
		 */
		public List<double[]> call(Row t) throws Exception {
			List<double[]> res = new ArrayList<double[]>();
			TablePartition tp = (TablePartition) t.rawdata();
			for (int i : columnList) {
				final DoubleBuffer buffer = tp.columns()[i].asDoubleBuffer();
				final double[] copy = new double[buffer.remaining()];
				buffer.get(copy);
				res.add(copy);
			}
			return res;
		}

	}
	
	/**
	 * Copy data out using buffer copy, null not filtered
	 * Output data is stored as a list of partitioned columns of primitive type
	 */
	static public class TP2IntegerPrimitiveArrayMapper extends Function<Row, List<int[]>> {
		int[] columnList;

		public TP2IntegerPrimitiveArrayMapper() {
			super();
		}

		public TP2IntegerPrimitiveArrayMapper(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
		/**
		 * Copy data out using IntBuffer, null not filtered		 
		 */
		public List<int[]> call(Row t) throws Exception {
			List<int[]> res = new ArrayList<int[]>();
			TablePartition tp = (TablePartition) t.rawdata();
			for (int i : columnList) {
				final IntBuffer buffer = tp.columns()[i].asIntBuffer();
				final int[] copy = new int[buffer.remaining()];
				buffer.get(copy);
				res.add(copy);
			}
			return res;
		}

	}
	
	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of partitioned columns of primitive type
	 */
	static public class TP2DoublePrimitiveArrayMapper2 extends Function<Row, List<double[]>> {
		int[] columnList;

		public TP2DoublePrimitiveArrayMapper2() {
			super();
		}

		public TP2DoublePrimitiveArrayMapper2(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		
		@Override
		public List<double[]> call(Row t) throws Exception {
			List<double[]> res = new ArrayList<double[]>();
			TablePartition tp = (TablePartition) t.rawdata();

			TablePartitionIterator ti = tp.iterator();
			for (int i = 0; i < columnList.length; i++) {
				res.add(new double[(int) ti.numRows()]);
			}
			int i = 0;
			LOG.info("Ti number of rows: " + ti.numRows());
			while (ti.hasNext()) {
				ColumnarStruct cs = ti.next();
				for (int j = 0; j < columnList.length; j++) {
					// if(cs.getField(columnList[j]) == null){
					// LOG.info("Get field: "+columnList[j]+": "+cs.getField(columnList[j]));
					// }
					Object dw = (DoubleWritable) cs.getField(columnList[j]);
					if (dw != null) {
						res.get(j)[i] = ((DoubleWritable) cs.getField(columnList[j])).get();
					} else {
						// res.get(j)[i] = null;
					}
				}
				i++;
			}
			return res;
		}
	}
	
	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of partitioned columns of primitive type
	 */
	static public class TP2IntegerPrimitiveArrayMapper2 extends Function<Row, List<int[]>> {
		int[] columnList;

		public TP2IntegerPrimitiveArrayMapper2() {
			super();
		}

		public TP2IntegerPrimitiveArrayMapper2(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		
		@Override
		public List<int[]> call(Row t) throws Exception {
			List<int[]> res = new ArrayList<int[]>();
			TablePartition tp = (TablePartition) t.rawdata();

			TablePartitionIterator ti = tp.iterator();
			for (int i = 0; i < columnList.length; i++) {
				res.add(new int[(int) ti.numRows()]);
			}
			int i = 0;
			LOG.info("Ti number of rows: " + ti.numRows());
			while (ti.hasNext()) {
				ColumnarStruct cs = ti.next();
				for (int j = 0; j < columnList.length; j++) {
					// if(cs.getField(columnList[j]) == null){
					// LOG.info("Get field: "+columnList[j]+": "+cs.getField(columnList[j]));
					// }
					IntWritable dw = (IntWritable) cs.getField(columnList[j]);
					if (dw != null) {
						res.get(j)[i] = ((IntWritable) cs.getField(columnList[j])).get();
					} else {
						// res.get(j)[i] = null;
					}
				}
				i++;
			}
			return res;
		}
	}

	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of partitioned columns of object type
	 */
	static public class TP2DoubleObjectArrayMapper extends Function<Row, List<Double[]>> {
		int[] columnList;

		public TP2DoubleObjectArrayMapper() {
			super();
		}

		public TP2DoubleObjectArrayMapper(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
		public List<Double[]> call(Row t) throws Exception {
			List<Double[]> res = new ArrayList<Double[]>();
			TablePartition tp = (TablePartition) t.rawdata();

			TablePartitionIterator ti = tp.iterator();
			for (int i = 0; i < columnList.length; i++) {
				res.add(new Double[(int) ti.numRows()]);
			}
			int i = 0;
			LOG.info("Ti number of rows: " + ti.numRows());
			while (ti.hasNext()) {
				ColumnarStruct cs = ti.next();
				for (int j = 0; j < columnList.length; j++) {
					// if(cs.getField(columnList[j]) == null){
					// LOG.info("Get field: "+columnList[j]+": "+cs.getField(columnList[j]));
					// }
					DoubleWritable dw = (DoubleWritable) cs.getField(columnList[j]);
					if (dw != null) {
						res.get(j)[i] = ((DoubleWritable) cs.getField(columnList[j])).get();
					} else {
						res.get(j)[i] = null;
					}
				}
				i++;
			}
			return res;
		}
	}

	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of partitioned columns of object type
	 */
	static public class TP2IntegerObjectArrayMapper extends Function<Row, List<Integer[]>> {
		int[] columnList;

		public TP2IntegerObjectArrayMapper() {
			super();
		}

		public TP2IntegerObjectArrayMapper(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
		public List<Integer[]> call(Row t) throws Exception {
			List<Integer[]> res = new ArrayList<Integer[]>();
			TablePartition tp = (TablePartition) t.rawdata();

			TablePartitionIterator ti = tp.iterator();
			for (int i = 0; i < columnList.length; i++) {
				res.add(new Integer[(int) ti.numRows()]);
			}
			int i = 0;
			LOG.info("Ti number of rows: " + ti.numRows());
			while (ti.hasNext()) {
				ColumnarStruct cs = ti.next();
				for (int j = 0; j < columnList.length; j++) {
					// if(cs.getField(columnList[j]) == null){
					// LOG.info("Get field: "+columnList[j]+": "+cs.getField(columnList[j]));
					// }
					IntWritable dw = (IntWritable) cs.getField(columnList[j]);
					if (dw != null) {
						res.get(j)[i] = ((IntWritable) cs.getField(columnList[j])).get();
					} else {
						res.get(j)[i] = null;
					}
				}
				i++;
			}
			return res;
		}
	}
	
	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of rows of object type
	 */
	static public class TP2DoubleObjectArrayMapper2 extends Function<Row, Object[]> {
		int[] columnList;

		public TP2DoubleObjectArrayMapper2() {
			super();
		}

		public TP2DoubleObjectArrayMapper2(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
		@SuppressWarnings("unused")
		public Object[] call(Row row) throws Exception {
			// TODO: this is a hacky way to get the row size. Unless we
			// change shark core, we have to live with it
			int len = row.colname2indexMap().size();
			Object[] res;

			res = new Object[columnList.length];
			for (int i = 0; i < columnList.length; i++) {
				res[i] = row.apply(i);
			}
			return res;
		}

	}
	
	/**
	 * Copy data out using getField, null filtered
	 * Output data is stored as a list of rows of object type
	 */
	static public class TP2IntegerObjectArrayMapper2 extends Function<Row, Object[]> {
		int[] columnList;

		public TP2IntegerObjectArrayMapper2() {
			super();
		}

		public TP2IntegerObjectArrayMapper2(int[] columnList) {
			super();
			this.columnList = columnList;
		}

		@Override
    @SuppressWarnings("unused")
		public Object[] call(Row row) throws Exception {
			// TODO: this is a hacky way to get the row size. Unless we
			// change shark core, we have to live with it
			int len = row.colname2indexMap().size();
			Object[] res;

			res = new Object[columnList.length];
			for (int i = 0; i < columnList.length; i++) {
				res[i] = row.apply(i);
			}
			return res;
		}

	}

	static public class DoublePrimitiveArrayReducer extends Function2<List<double[]>, List<double[]>, List<double[]>> {

		@Override
		public List<double[]> call(List<double[]> tl1, List<double[]> tl2) throws Exception {
			double sum = 0.0;
			List<double[]> res = new ArrayList<double[]>();

			for (int i = 0; i < tl1.size(); i++) {
				sum = 0.0;
				double[] t1 = tl1.get(i);
				double[] t2 = tl2.get(i);
				for (int j = 0; j < t1.length; j++) {
					sum += t1[j];
				}

				for (int j = 0; j < t2.length; j++) {
					sum += t2[j];
				}
				double[] a = { sum };
				res.add(a);
			}
			return res;
		}
	}

	static public class DoubleObjectArrayReducer extends Function2<List<Double[]>, List<Double[]>, List<Double[]>> {

		@Override
		public List<Double[]> call(List<Double[]> tl1, List<Double[]> tl2) throws Exception {
			Double sum = 0.0;
			List<Double[]> res = new ArrayList<Double[]>();

			for (int i = 0; i < tl1.size(); i++) {
				sum = 0.0;
				Double[] t1 = tl1.get(i);
				Double[] t2 = tl2.get(i);
				for (int j = 0; j < t1.length; j++) {
					sum += t1[j];
				}

				for (int j = 0; j < t2.length; j++) {
					sum += t2[j];
				}
				Double[] a = { sum };
				res.add(a);
			}
			return res;
		}
	}

	static public class IntegerPrimitiveArrayReducer extends Function2<List<int[]>, List<int[]>, List<int[]>> {

		@Override
		public List<int[]> call(List<int[]> tl1, List<int[]> tl2) throws Exception {
			int sum = 0;
			List<int[]> res = new ArrayList<int[]>();

			for (int i = 0; i < tl1.size(); i++) {
				sum = 0;
				int[] t1 = tl1.get(i);
				int[] t2 = tl2.get(i);
				for (int j = 0; j < t1.length; j++) {
					sum += t1[j];
				}

				for (int j = 0; j < t2.length; j++) {
					sum += t2[j];
				}
				int[] a = { sum };
				res.add(a);
			}
			return res;
		}
	}

	static public class IntegerObjectArrayReducer extends Function2<List<Integer[]>, List<Integer[]>, List<Integer[]>> {

		@Override
		public List<Integer[]> call(List<Integer[]> tl1, List<Integer[]> tl2) throws Exception {
			Integer sum = 0;
			List<Integer[]> res = new ArrayList<Integer[]>();

			for (int i = 0; i < tl1.size(); i++) {
				sum = 0;
				Integer[] t1 = tl1.get(i);
				Integer[] t2 = tl2.get(i);
				for (int j = 0; j < t1.length; j++) {
					sum += t1[j];
				}

				for (int j = 0; j < t2.length; j++) {
					sum += t2[j];
				}
				Integer[] a = { sum };
				res.add(a);
			}
			return res;
		}
	}

	//@Test
	public void benchmarkLoadHiveTableMtcars() throws Exception {
		int maxIter = 2;
		String[] jobJars = RSERVER_JAR.split(",");
		JavaSharkContext sharkContext = SharkEnv.initWithJavaSharkContext(new JavaSharkContext(SPARK_MASTER, "BigR", SPARK_HOME, jobJars));
//		System.setProperty("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
//		System.setProperty("spark.kryo.registrator", shark.KryoRegistrator.class.getName());
		String inputPath = System.getProperty("inputPath", "resources/sharkfiles/mtcars/");
		sharkContext.sql("drop table if exists test");
		sharkContext
				.sql("CREATE EXTERNAL TABLE test (mpg DOUBLE, cyl DOUBLE, disp DOUBLE, hp DOUBLE, drat DOUBLE, wt DOUBLE, qsec DOUBLE, vs DOUBLE, "
						+ "am DOUBLE, gear DOUBLE, carb DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY \" \"" + "STORED AS TEXTFILE LOCATION \'"
						+ inputPath + "\'");
		// sharkContext.sql("LOAD DATA INPATH \'" + inputPath +
		// "\' INTO TABLE test");
		Long startTime;
		JavaTableRDD rowTablePartitionRDD = null;

		// for (int i = 0; i < maxIter; i++) {
		sharkContext.sql("drop table if exists testRDD");
		startTime = System.currentTimeMillis();
		rowTablePartitionRDD = sharkContext
				.sql2rdd("CREATE TABLE testRDD TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_ONLY\") "
						+ "AS SELECT * FROM test");
		LOG.info("[Execution time]  {}: {} ", "Creating shark cache table", Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000)
				+ "s");
		// }

		LOG.info(" ");
		int[] columnList = { 1, 2 };

		JavaRDD<List<double[]>> tpsDouble;
		for (int i = 0; i < maxIter; i++) {
			tpsDouble = rowTablePartitionRDD.map(new TP2DoublePrimitiveArrayMapper(columnList));
			startTime = System.currentTimeMillis();
			tpsDouble.count();
			LOG.info("[Execution time]  {}: {} ", "Creating JavaRDD<List<double[]>>s from cache table using ByteBuffer, null not filtered",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
			// tpsDouble.reduce(new DoubleArrayReducer());
		}
		LOG.info(" ");
		// JavaRDD<List<double[]>> tpsObject;
		// List<List<Double[]>> cl = tpsObject.collect();
		for (int i = 0; i < maxIter; i++) {
			tpsDouble = rowTablePartitionRDD.map(new TP2DoublePrimitiveArrayMapper2(columnList));
			startTime = System.currentTimeMillis();
			tpsDouble.count();
			LOG.info("[Execution time]  {}: {} ", "Creating JavaRDD<List<double[]>> from cache table using getField, null filtered",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
			// tpsObject.reduce(new DoubleObjectArrayReducer());
		}
		LOG.info(" ");

		for (int i = 0; i < 5; i++) {
			sharkContext.sql("drop table if exists testRDD2");
			startTime = System.currentTimeMillis();
			rowTablePartitionRDD = sharkContext
					.sql2rdd("CREATE TABLE testRDD2 TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_ONLY\") "
							+ "AS SELECT cyl,disp FROM testRDD");
			LOG.info("[Execution time] {}: {} ", "Creating JavaTableRDD<tablepartition> with column selection from cache table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info(" ");

		// Measure select steps
		JavaTableRDD rowTableRDD;
		for (int i = 0; i < maxIter; i++) {
			startTime = System.currentTimeMillis();
			rowTableRDD = sharkContext.sql2rdd("SELECT cyl,disp FROM testRDD");
			rowTableRDD.count();
			LOG.info("[Execution time] {}: {} ", "Creating JavaTableRDD<Row> with column selection from cached table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info(" ");

		// Cache result of select command to measure the conversion step only
		// rowTableRDD =
		// sharkContext.sql2rdd("SELECT cyl,disp FROM test").cache();
		// rowTableRDD.count();

		rowTableRDD = sharkContext.sql2rdd("SELECT cyl,disp FROM testRDD").cache();
		rowTableRDD.count();
		JavaRDD<Object[]> rowRDD;
		for (int i = 0; i < maxIter; i++) {
			startTime = System.currentTimeMillis();
			rowRDD = rowTableRDD.map(new TP2DoubleObjectArrayMapper2(columnList));
			rowRDD.count();
			LOG.info("[Execution time] {}: {} ", "Creating JavaRDD<Object[]> with column selection from cached table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info(" ");

		// JavaTableRDD rowRDD =
		// sharkContext.sql2rdd("SELECT cyl,disp FROM test").cache();
		// rowRDD.count();
	}

	//@Test
	public void benchmarkLoadHiveTableOntime() throws Exception {
		int maxIter = 5;
		int maxIterWithCache = 2;
		String[] jobJars = RSERVER_JAR.split(",");
		JavaSharkContext sharkContext = SharkEnv.initWithJavaSharkContext(new JavaSharkContext(SPARK_MASTER, "BigR", SPARK_HOME, jobJars));
//		System.setProperty("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
//		System.setProperty("spark.kryo.registrator", shark.KryoRegistrator.class.getName());
		String inputPath = System.getProperty("inputPath", "resources/sharkfiles/ontime/");
		sharkContext.sql("drop table if exists test");
		sharkContext
				.sql("CREATE EXTERNAL TABLE test (year	INT, month	INT, dayofmonth	INT, dayofweek	INT, deptime	INT, crsdeptime	INT," +
						" arrtime	INT, crsarrtime	INT, uniquecarrier	string	, flightnum	INT, tailnum	string	, actualelapsedtime	INT, " +
						"crselapsedtime	INT, airtime	INT, arrdelay	INT, depdelay	INT, origin	string	, dest	string	, distance	INT, " +
						"taxiin	INT, taxiout	INT, cancelled	INT, cancellationcode	string	, diverted	string	, carrierdelay	INT, " +
						"weatherdelay	INT, nasdelay	INT, securitydelay	INT, lateaircraftdelay	INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" " +
						"STORED AS TEXTFILE LOCATION \'"+ inputPath+"\'");
//		sharkContext.sql("LOAD DATA INPATH \'" + inputPath + "\' INTO TABLE test");
		Long startTime;
		JavaTableRDD rowTablePartitionRDD = null;
		
//		for (int i = 0; i < maxIter; i++) {
			sharkContext.sql("drop table if exists testRDD");	
			startTime = System.currentTimeMillis();
			rowTablePartitionRDD = sharkContext
					.sql2rdd("CREATE TABLE testRDD TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_ONLY\") "
							+ "AS SELECT * FROM test");
			LOG.info("[Execution time]  {}: {} ", "Creating shark cache table", Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000)
					+ "s");
//		}
		
		LOG.info("");
		int[] columnList = { 1, 2, 3, 4, 5 };
		long count=0;
		
		JavaRDD<List<int[]>> tpsInteger;
		for (int i = 0; i < maxIter; i++) {
			tpsInteger = rowTablePartitionRDD.map(new TP2IntegerPrimitiveArrayMapper(columnList));
			startTime = System.currentTimeMillis();
			count = tpsInteger.count();
			LOG.info("[Execution time]  {}: {} ", "Creating JavaRDD<List<int[]>>s from cache table using ByteBuffer, null not filtered",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
			// tpsInteger.reduce(new IntegerArrayReducer());
		}
		LOG.info("Count {}", count);
		
		// JavaRDD<List<int[]>> tpsObject;
		// List<List<Integer[]>> cl = tpsObject.collect();
		for (int i = 0; i < maxIter; i++) {
			tpsInteger = rowTablePartitionRDD.map(new TP2IntegerPrimitiveArrayMapper2(columnList));
			startTime = System.currentTimeMillis();
			count = tpsInteger.count();
			LOG.info("[Execution time]  {}: {} ", "Creating JavaRDD<List<int[]>> from cache table using getField, null filtered",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
			// tpsObject.reduce(new IntegerObjectArrayReducer());
		}
		LOG.info("Count {}", count);
		
		for (int i = 0; i < maxIterWithCache; i++) {
			sharkContext.sql("drop table if exists testRDD2");
			startTime = System.currentTimeMillis();
			rowTablePartitionRDD = sharkContext
					.sql2rdd("CREATE TABLE testRDD2 TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_ONLY\") "
							+ "AS SELECT year, month, dayofmonth, dayofweek, deptime FROM testRDD");
			LOG.info("[Execution time] {}: {} ", "Creating JavaTableRDD<tablepartition> with column selection from cache table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info("");
		
		// Measure select steps
		JavaTableRDD rowTableRDD;
		for (int i = 0; i < maxIter; i++) {
			startTime = System.currentTimeMillis();
			rowTableRDD = sharkContext.sql2rdd("SELECT year, month, dayofmonth, dayofweek, deptime FROM testRDD");
			count = rowTableRDD.count();
			LOG.info("[Execution time] {}: {} ", "Creating JavaTableRDD<Row> with column selection from cached table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info("Count {}", count);
		
		// Cache result of select command to measure the conversion step only
		// rowTableRDD =
		// sharkContext.sql2rdd("SELECT cyl,disp FROM test").cache();
		// rowTableRDD.count();
		
		rowTableRDD = sharkContext.sql2rdd("SELECT year, month, dayofmonth, dayofweek, deptime FROM testRDD").cache();
		rowTableRDD.count();
		JavaRDD<Object[]> rowRDD;
		for (int i = 0; i < maxIter; i++) {
			startTime = System.currentTimeMillis();
			rowRDD = rowTableRDD.map(new TP2IntegerObjectArrayMapper2(columnList));
			count = rowRDD.count();
			
			LOG.info("[Execution time] {}: {} ", "Creating JavaRDD<Object[]> with column selection from cached table",
					Double.toString((System.currentTimeMillis() - startTime) * 1.0 / 1000) + "s");
		}
		LOG.info("Count {}", count);
		
		// JavaTableRDD rowRDD =
		// sharkContext.sql2rdd("SELECT cyl,disp FROM test").cache();
		// rowRDD.count();
	}
}
