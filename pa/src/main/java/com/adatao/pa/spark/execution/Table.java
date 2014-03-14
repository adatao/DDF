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

import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;
import shark.api.JavaSharkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.SharkColumnVector;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

// For prototype/templating purpose
// This executor returns the FULL result of a query as List<String>
@SuppressWarnings("serial")
public class Table extends CExecutor {
	String dataContainerID;

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
		if (dc.getType() == DataContainer.ContainerType.DataFrame) {
			List<Tuple2<String, Integer>> output = dc.getRDD()
					.map(new PairFunction<Object[], String, Integer>() {
						@Override
						public Tuple2<String, Integer> call(Object[] arg0)
								throws Exception {
							String s = "NULL";
							if (arg0[0] != null)
								s = arg0[0].toString();
							Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(
									s, 1);
							return tuple;
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						@Override
						public Integer call(Integer arg0, Integer arg1)
								throws Exception {
							return arg0 + arg1;
						}
					}).collect();
			
			List<String> res = new ArrayList<String>(output.size());
			for(int i = 0; i < output.size(); i++) {
				Tuple2<String, Integer> t = output.get(i);
				res.add(t._1 + "\t" + t._2);
			}
			return new Sql2ListStringResult().setResults(res);
			
		} else if (dc.getType() == DataContainer.ContainerType.SharkColumnVector) {
			SharkColumnVector dv = (SharkColumnVector) dc;
			JavaSharkContext sc = (JavaSharkContext) sparkThread
					.getSparkContext();
			String colName = dv.getColumn();
			List<String> res = sc.sql(String.format(
					"SELECT %s, COUNT(*) count FROM %s GROUP BY %s", colName,
					dv.tableName, colName));
			return new Sql2ListStringResult().setResults(res);
		} else {
			return new FailResult().setMessage("bad DataContainer.type: "
					+ dc.getType());
		}
	}
	
}
