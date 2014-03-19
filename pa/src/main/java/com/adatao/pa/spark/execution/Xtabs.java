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

import java.util.List;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import shark.api.JavaSharkContext;

// For prototype/templating purpose
// This executor returns the FULL result of a query as List<String>
@SuppressWarnings("serial")
public class Xtabs extends CExecutor {
	String dataContainerID;
	String gcols;
	String scols;

	static public class Sql2ListStringResult extends SuccessResult {
		List<String> results;

		public Sql2ListStringResult setResults(List<String> results) {
			this.results = results;
			return this;
		}

		public List<String> getResults() {
			return results;
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
		SharkDataFrame df = (SharkDataFrame) dc;
		JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
		String sqlStr;
		if (!scols.equalsIgnoreCase("count")) {
			sqlStr = String.format("SELECT %s, SUM(%s) %s FROM %s GROUP BY %s",
					gcols, scols, scols, df.tableName, gcols);
		} else {
			sqlStr = String.format(
					"SELECT %s, COUNT(*) count FROM %s GROUP BY %s", gcols,
					df.tableName, gcols);
		}
		List<String> res = sc.sql(sqlStr);
		return new Sql2ListStringResult().setResults(res);
	}

	public Xtabs setSqlCmd(String dataContainerID, String gcols, String scols) {
		this.dataContainerID = dataContainerID;
		this.gcols = gcols;
		this.scols = scols;
		return this;
	}
}
