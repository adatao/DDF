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


import java.io.Serializable;
import javax.script.ScriptException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import com.adatao.pa.spark.MapReduce;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class RunMapReduceTextFile implements IExecutor, Serializable {
	private String input = null;
	private String output = null;
	private String map = null;
	private String reduce = null;
	private String combine = null;
	private String data = null;

	static public class RunMapReduceTextFileResult extends SuccessResult{
		String result;

		public String getResult() {
			return result;
		}

		public RunMapReduceTextFileResult setResult(String result) {
			this.result = result;
			return this;
		}

	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		try {
			// Check MapReduce R syntax
			MapReduce.getInstance().checkMapReduceFunc(map, reduce, combine);
			JavaRDD<String> lines = sparkThread.getSparkContext()
					.textFile(input);
			JavaPairRDD<String, String> resPair = MapReduce.getInstance()
					.runMapReduceWithHadoopReducer(lines, map, reduce, combine, data);
			//resPair.saveAsTextFile(output);
			JavaRDD<String> res = resPair.map(new Function<Tuple2<String, String>, String>() {
				public String call( Tuple2<String, String> t) throws Exception {
					return t._1 + "," + t._2;
				}
			}
			);
			System.out.println(res.take(10).toString());
			res.saveAsTextFile(output);
			
			return new RunMapReduceTextFileResult().setResult(output);
		} catch (ScriptException e) {
			return new FailResult().setMessage(e.getMessage());
		} catch (Exception e) {
			return new FailResult().setMessage(e.getMessage());
		}
	}

}
