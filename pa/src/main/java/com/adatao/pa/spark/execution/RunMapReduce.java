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

import java.util.Iterator;
import java.util.Map;
import javax.script.ScriptException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class RunMapReduce extends CExecutor {
	private String file = null;
	private String map = null;
	private String reduce = null;
	private String combine = null;
	private int count = 10;

//	static public class RunMapReduceResult extends SuccessResult {
//		String result;
//
//		public String getResult() {
//			return result;
//		}
//
//		public RunMapReduceResult setResult(String result) {
//			this.result = result;
//			return this;
//		}
//
//	}
//
//	@Override
//	public ExecutorResult run(SparkThread sparkThread) {
//		try {
//			// Check MapReduce R syntax
//			MapReduce.getInstance().checkMapReduceFunc(map, reduce, combine);
//			JavaRDD<String> lines = sparkThread.getSparkContext()
//					.textFile(file);
//			JavaPairRDD<String, String> resPair = MapReduce.getInstance()
//					.runMapReduceWithHadoopReducer(lines, map, reduce, combine, null);
//
//			Map<String, String> map = resPair.collectAsMap();
//			Iterator<String> ite = map.keySet().iterator();
//			StringBuffer values = new StringBuffer();
//			int i = 0;
//			while (ite.hasNext() && i < count) {
//				String key = ite.next();
//				String value = map.get(key);
//				values.append(key + "," + value + "\n");
//				i++;
//			}
//			return new RunMapReduceResult().setResult(values.toString());
//		} catch (ScriptException e) {
//			return new RunMapReduceResult().setResult(e.getMessage());
//		} catch (Exception e) {
//			return new RunMapReduceResult().setResult(e.getMessage());
//		}
//	}

}
