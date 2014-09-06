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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import io.spark.ddf.util.MLUtils;
import io.ddf.types.Matrix;
import io.ddf.types.Vector;
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
