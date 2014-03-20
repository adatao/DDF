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

package com.adatao.pa.spark.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import shark.api.JavaSharkContext;
@SuppressWarnings("serial")
public class LoadHiveTable extends CExecutor {

	String tableName;
	Boolean cache = true;
//	String[] 

	public static Logger LOG = LoggerFactory.getLogger(LoadHiveTable.class);

	// TODO: support storageLevel
	// String storageLevel

	public static class LoadHiveTableResult extends SuccessResult {
		public String dataContainerID;
		public MetaInfo[] metaInfo;
		public String tableName;
		
		public LoadHiveTableResult(String dataContainerID, SharkDataFrame df) {
			this.dataContainerID = dataContainerID;
			this.metaInfo = df.getMetaInfo();
			this.tableName = df.getTableName();
		}
		
		public String getDataContainerID() {
			return dataContainerID;
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		SharkDataFrame df = new SharkDataFrame();
		JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
		df.loadTable(sc, tableName, cache);
		
		DataManager dm = sparkThread.getDataManager();
		String dataContainerID = dm.add(df);

		return new LoadHiveTableResult(dataContainerID, df);
	}

	public LoadHiveTable setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public LoadHiveTable setCache(Boolean cache) {
		this.cache = cache;
		return this;
	}

}
