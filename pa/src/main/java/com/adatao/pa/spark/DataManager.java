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

package com.adatao.pa.spark;

import java.io.Serializable;
import java.util.*;
import adatao.ML.Kmeans;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Int;
import scala.Tuple2;
import scala.Unit;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifest$;
import shark.SharkConfVars;
import shark.SharkEnv;
import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;
import shark.api.ColumnDesc;
import shark.api.Row;
import shark.memstore2.TablePartition;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import adatao.ML.types.Matrix;
import adatao.ML.types.Vector;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult;

@SuppressWarnings("serial")
public class DataManager {
	public static Logger LOG = LoggerFactory.getLogger(DataManager.class);
	private HashMap<String, DataContainer> dataContainers = new HashMap<String, DataContainer>();

	// cache for data that can be used accross different dataframes,
	// such as persisted models
	private HashMap<String, Object> persistedObjects = new HashMap<String, Object>();

	public static class MetaInfo implements Serializable {

		String header = null;
		String type;
		int columnNo = -1; // unset, base-1

		// does this belongs to metainfo? should belong to DataContainer
		// this is really a cached result of some computation
		Map<String, Integer> factor;

		public MetaInfo(String header, String type) {
			this.header = header;
			this.type = type;
		}

		public MetaInfo(String header, String type, int colNo) {
			this(header, type);
			this.columnNo = colNo;
		}

		public String getHeader() {
			return header;
		}

		public MetaInfo setHeader(String header) {
			this.header = header;
			return this;
		}

		public String getType() {
			return type;
		}

		public MetaInfo setType(String type) {
			this.type = type;
			return this;
		}

		public int getColumnNo() {
			return this.columnNo;
		}

		public MetaInfo setColumnNo(int colNo) {
			this.columnNo = colNo;
			return this;
		}

		@Override
		public String toString() {
			return "MetaInfo [header=" + header + ", type=" + type + ", columnNo=" + columnNo + ", hasFactor=" + hasFactor() + "]";
		}

		public Map<String, Integer> getFactor() {
			return factor;
		}

		public MetaInfo setFactor(Map<String, Integer> factor) {
			this.factor = factor;
			return this;
		}

		public Boolean hasFactor() {
			return factor != null ? true : false;
		}

		public MetaInfo clone() {
			return new MetaInfo(header, type, columnNo).setFactor(factor);
		}
	}

	public static abstract class DataContainer {

		MetaInfo[] metaInfo; // the size is 1 for Vector

		// cached computation results per DataFrame,
		// we assume that each dataframe is immutable so that computation result does not change
		public HashMap<String, Object> cache = new HashMap<String, Object>();

		public enum ContainerType {
			DataFrame, SharkDataFrame, SharkColumnVector
		}

		protected ContainerType type;

		protected String uid;

		public DataContainer() {
			uid = UUID.randomUUID().toString();
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}

		public void setNames(String[] names) {
			int length = names.length < metaInfo.length ? names.length : metaInfo.length;
			for (int i = 0; i < length; i++) {
				metaInfo[i].setColumnNo(i).setHeader(names[i]);
			}
		}

		public MetaInfo getColumnMetaInfoByIndex(int i) {
			if (metaInfo == null) {
				return null;
			}
			if (i < 0 || i >= metaInfo.length) {
				return null;
			}

			return metaInfo[i];
		}

		public MetaInfo getColumnMetaInfoByName(String name) {
			Integer i = getColumnIndexByName(name);
			if (i == null) {
				return null;
			}

			return getColumnMetaInfoByIndex(i);
		}

		public Integer getColumnIndexByName(String name) {
			if (metaInfo == null) {
				return null;
			}
			for (int i = 0; i < metaInfo.length; i++) {
				if (metaInfo[i].getHeader().equals(name)) {
					return i;
				}
			}
			return null;
		}

		public abstract JavaRDD<Object[]> getRDD();

		public ContainerType getType() {
			return type;
		}

		public String getUid() {
			return uid;
		}

		public DataContainer setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}

		public String putQuickSummary(DataframeStatsResult obj) {
			assert obj.getDataContainerID().equals(this.uid);
			String key = "QuickSummary:" + obj.getDataContainerID();
			cache.put(key, obj);
			return key;
		}

		public DataframeStatsResult getQuickSummary() {
			return (DataframeStatsResult) cache.get("QuickSummary:" + this.uid);
		}

		/**
		 * Return cached value of column mean, null otherwise.
		 */
		public Double getColumnMean(int col) {
			Double m = null;
			DataframeStatsResult s = getQuickSummary();
			if (s != null) {
				 m = s.mean[col];
			}
			return m;
		}

		/**
		 * Return cached value of column mean, null otherwise.
		 */
		public Double getColumnMean(String col) {
			// XXX: can't use column index because it is not set on SharkTable
			int i;
			boolean found = false;
			for(i=0; i<metaInfo.length; i++) {
				if (metaInfo[i].getHeader().equals(col)) {
					found = true;
					break;
				}
			}
			if (!found) throw new IllegalArgumentException("column does not exists");
			return getColumnMean(i);
		}
	}

	/**
	 * This is class representation of a DataFrame loaded from a Shark table
	 * <p>
	 * version 2: The main data is stored as a JavaTableRDD where each row is a
	 * TablePartition
	 * <p>
	 * version 1: The main data is stored as a JavaTableRDD where each row is a
	 * row in the original table
	 * 
	 * @author bachbui
	 * 
	 */
	public static class SharkDataFrame extends DataContainer {
		
		@Deprecated
		public JavaTableRDD table;
		
		public String tableName;

		/**
		 * {@code rawdata} of each {@code Row} in {@code tablePartitionRDD} is a
		 * TablePartition, whereas {@code rawdata} of each {@code Row} in
		 * {@code table} is a row in the original table
		 * 
		 */
		private RDD<TablePartition> tablePartitionRDD;

		public SharkDataFrame(RDD<TablePartition> tablePartitionRDD, MetaInfo[] metaInfo) {
			this();
			this.tablePartitionRDD = tablePartitionRDD;
			this.metaInfo = metaInfo.clone();
		}
		
		// Initialize SharkDataFrame with a uid and name,
		// so that the table reference can be set later.
		public SharkDataFrame() {
			super();
			this.tableName = ("bigrdf" + this.uid).replace('-', '_');
			this.type = ContainerType.SharkDataFrame;
		}

		public SharkDataFrame setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public SharkDataFrame setRDD(JavaTableRDD table) {
			this.table = table;
			return this;
		}

		public RDD<TablePartition> getTablePartitionRDD() {
			return this.tablePartitionRDD;
		}

		@Deprecated
		public JavaTableRDD getTableRDD() {
			return this.table;
		}

		public static class MapRow extends Function<Row, Object[]> {
			ColumnDesc[] schema;
//			List<FieldSchema> schema;
//			public MapRow(List<FieldSchema> schema) {
			public MapRow(ColumnDesc[] schema) {
				this.schema = schema.clone();
			}
			@Override
			public Object[] call(Row row) throws Exception {
				int len = this.schema.length;
				Object[] res = new Object[len];
				for (int i = 0; i < len; i++) {
					res[i] = row.apply(i);
				}

				return res;
			}
		}

		public JavaRDD<Object[]> getRDD() {
			return this.table.map(new MapRow(this.table.schema()));
		}

		public String getTableName() {
			return tableName;
		}

		/**
		 * 
		 * @param sparkThread
		 * @param tableName
		 * @param cache
		 */
		public SharkDataFrame loadTable(JavaSharkContext sparkContext, String hiveTableName, Boolean cache) {
			return loadTableFromQuery(sparkContext, "SELECT * FROM "+hiveTableName, cache);
		}

		public SharkDataFrame loadTableFromQuery(JavaSharkContext sparkContext, String subsetCmd, Boolean cache) {
			String sqlCmd;

			if (cache) {
				sqlCmd = String.format(
						"CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_AND_DISK\") AS %s",
						this.tableName, subsetCmd);
			} else {
				sqlCmd = String.format("CREATE TABLE %s AS %s",
						this.tableName, subsetCmd);
			}
			LOG.info("Creating a cached table from a hive table: " + sqlCmd);
			// TODO: check if  tablePartitionRDD already exist, if so we should refuse to assign new RDD to it
			// otherwise we will get cached RDD leak
			tablePartitionRDD = sparkContext.sql2rdd(sqlCmd).map(new TablePartitionMapper()).rdd();

			/**
			 * This is for backward compatible with algorithms that use
			 * {@code table} to access data
			 * TODO: remove this when all algorithm move to use tablePartitionRDD instead
			 */
			sqlCmd = String.format("SELECT * FROM %s", this.tableName);
			table = sparkContext.sql2rdd(sqlCmd);

			// Inherits metaInfo from Hive schema
			ColumnDesc[] fs = table.schema();
			this.metaInfo = new MetaInfo[fs.length];
			for (int i = 0; i < fs.length; i++) {
				this.metaInfo[i] = new MetaInfo(fs[i].columnName(), fs[i].typeName(), i);
			}

			return this;
		}
		
		static public class TablePartitionMapper extends Function<Row, TablePartition> {

			public TablePartitionMapper() {
				super();
			}

			@Override
			public TablePartition call(Row t) throws Exception {
				return  (TablePartition) t.rawdata();
			}
		}
		
		public RDD<Tuple2<Matrix, Vector>> getDataTable(int[] xCols, int yCol){
			String key = String.format("xytable:%s:%s", Arrays.toString(xCols), yCol);
			RDD<Tuple2<Matrix, Vector>> result = (RDD<Tuple2<Matrix, Vector>>) cache.get(key);
			if (result == null) {
				RDD<TablePartition> tpRDD = this.tablePartitionRDD;
				result = SharkDataFrameScala.getDataTable(tpRDD, xCols, yCol, null);

				cache.put(key, result);
			}
			return result;
		}
		public RDD<double[]> getDataPointTable(int[] xCols){
			String key = String.format("xdatapointtable:%s", Arrays.toString(xCols));
			RDD<double[]> result =(RDD<double[]>) cache.get(key);
			if(result == null){
				RDD<Row> rdd = this.getTableRDD().rdd();

				Kmeans.SharkParsePoint parser = new Kmeans.SharkParsePoint(xCols, this.metaInfo);
				//ClassManifest$.MODULE$.fromClass
				result = rdd.map(parser, ClassManifest$.MODULE$.fromClass(double[].class)).
						filter(new Kmeans.filterFunction()).cache();
				cache.put(key, result);
			}
			return result;
		}

		public RDD<Tuple2<Matrix, Vector>> getDataTableCategorical(int[] xCols, int yCol, Map<Integer, HashMap<String, Double>> hm){
			String key = String.format("xytable:%s:%s", Arrays.toString(xCols), yCol);
			RDD<Tuple2<Matrix, Vector>> result = (RDD<Tuple2<Matrix, Vector>>) cache.get(key);
			
			if (result == null) {
				RDD<TablePartition> tpRDD = this.tablePartitionRDD;
				result = SharkDataFrameScala.getDataTable(tpRDD, xCols, yCol, hm);

				cache.put(key, result);
			}
			
			return result;
		}
		

		/**
		 * Perform a transformation on the dataset by applying a SQL statement,
		 * returning a new SharkDataFrame.
		 *
		 * NOTE: the transformed must be added to DataManager by caller.
		 *
		 * NOTE: preliminary benchmark result is that the cost of CREATE TABLE
		 * and INSERT OVERWRITE for transformation is the same.
		 */
		public SharkDataFrame transform(JavaSharkContext sparkContext, String sql, Boolean cache, Boolean keepFactors) {
			MetaInfo[] oldMeta = new MetaInfo[this.metaInfo.length];
			for (int i=0; i<oldMeta.length; i++) {
				oldMeta[i] = this.metaInfo[i].clone();
			}

			SharkDataFrame df = new SharkDataFrame();
			df.loadTableFromQuery(sparkContext, sql, cache);

			if (keepFactors) {
				for (int i=0; i<oldMeta.length; i++) {
					if (oldMeta[i].hasFactor()) this.metaInfo[i].setFactor(oldMeta[i].getFactor());
				}
			}
			return df;
		}
	}


	public static class DataFrame extends DataContainer {

		JavaRDD<Object[]> table;

		public DataFrame(MetaInfo[] metaInfo, JavaRDD<Object[]> table) {
			super();
			this.metaInfo = metaInfo.clone();
			this.table = table;
			this.type = ContainerType.DataFrame;
		}

		public JavaRDD<Object[]> getRDD() {
			return table;
		}

		/**
		 * Perform a transformation on the dataset by applying a map function.
		 * returning a new DataFrame.
		 *
		 * NOTE: the transformed must be added to DataManager by caller.
		 */
		public DataFrame transform(Function<Object[], Object[]> fn, Boolean keepFactors) {
			MetaInfo[] newMeta = new MetaInfo[this.metaInfo.length];
			for (int i=0; i<newMeta.length; i++) {
				newMeta[i] = this.metaInfo[i].clone();
			}

			DataFrame df = new DataFrame(newMeta, this.table.map(fn));

			// clear out the computed factors as data has changed, unless keepFactors = T
			if (!keepFactors) {
				for (MetaInfo colmeta : df.metaInfo) {
					colmeta.factor = null;
				}
			}
			return df;
		}

	}
	
	
	
	// SharkColumnVector is a simply a column in a Shark table
 	public static class SharkColumnVector extends SharkDataFrame {
		String column;

		public SharkColumnVector() {
			super();
			this.tableName = null;
			this.type = ContainerType.SharkColumnVector;
		}

		public SharkColumnVector setColumn(String column) {
			this.column = column;
			return this;
		}

		// Initialize a SharkColumnVector that is just a column of a SharkDataFrame
		public static SharkColumnVector fromSharkDataFrame(SharkDataFrame sdf, String colName) {
			SharkColumnVector v = new SharkColumnVector();
			LOG.info("extracting column vector from original table: {} from {}", colName, sdf.getTableName());
			v.setRDD(sdf.getTableRDD());
			v.setTableName(sdf.getTableName());
			v.setColumn(colName);
			v.setMetaInfo(new MetaInfo[] {sdf.getColumnMetaInfoByName(colName)});
			return v;
		}

		public String getColumn() {
			return column;
		}

		// we cannot reuse SharkDataFrame.MapRow because the underlying tableRDD and schema 
		// is the same between SharkDataFrame and its SharkColumnVector,
		// and we want to return values from the selected column only.
		// TODO: improve this	conversion between SharkDataFrame and RDD<Object[]> as a whole.
		public static class MapRow extends Function<Row, Object[]> {
			String column, type;
			public MapRow(String column, String type) {
				this.column = column;
				this.type = type;
			}
			@Override
			public Object[] call(Row row) throws Exception {
				Object[] res = new Object[1];
				res[0] = row.apply(column);

				return res;
			}
		}

		public JavaRDD<Object[]> getRDD() {
			return this.table.map(new MapRow(column, this.getMetaInfo()[0].getType()));
		}
	}

	// NB: CSV-HDFS vector is currently a DataFrame one column,
	// TODO: optimize it.

	public String add(DataContainer dataCont) {
		LOG.info("adding DataContainer with id = {}, metaInfo = {}", dataCont.uid, Arrays.toString(dataCont.metaInfo));
		dataContainers.put(dataCont.uid, dataCont);
		return dataCont.uid;
	}

	public DataContainer get(String uid) {
		DataContainer dc = dataContainers.get(uid);
		if (dc != null) {
			LOG.info("found DataContainer with id = {}, metaInfo = {}", dc.uid, Arrays.toString(dc.metaInfo));
		}
		return dc;
	}

	public HashMap<String, DataContainer> getDataContainers() {
		return dataContainers;
	}
	
	/**
	 * Save a given object in memory for later (quick server-side) retrieval
	 * 
	 * @param obj
	 * @return
	 */
	public String putObject(Object obj) {
		String id = UUID.randomUUID().toString();
		persistedObjects.put(id, obj);
		return id;
	}

	/**
	 * Retrieve an earlier saved object given its ID
	 * 
	 * @param uid
	 * @return
	 */
	public Object getObject(String uid) {
		return persistedObjects.get(uid);
	}

}
