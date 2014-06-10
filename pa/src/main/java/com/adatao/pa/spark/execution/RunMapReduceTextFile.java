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
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;

//Note: we no longer need it, if you want to know it, please check at BigR.
@SuppressWarnings("serial")
public class RunMapReduceTextFile { //implements IExecutor, Serializable {
	private String input = null;
	private String output = null;
	private String map = null;
	private String reduce = null;
	private String combine = null;
	private String data = null;
}
