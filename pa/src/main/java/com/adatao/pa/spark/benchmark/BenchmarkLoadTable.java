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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.execution.LoadTable;

@SuppressWarnings("serial")
public class BenchmarkLoadTable implements Serializable {

	public static Logger LOG = LoggerFactory
			.getLogger(BenchmarkLoadTable.class);

	public static void main(String[] args) throws Exception {
		long time = System.currentTimeMillis();
		final String[] JOB_JAR = { "target/rserver-0.1.0.jar",
				"lib_managed/gson-2.2.2.jar" };

		JavaSparkContext sc = new JavaSparkContext(
				"mesos://10.232.52.181:5050", "BigR", "/root/spark", JOB_JAR);

		LoadTable lt = new LoadTable();
		String fileURL = "hdfs://10.232.52.181:9000/airline.csv";
		JavaRDD<String> fileRDD = sc.textFile(fileURL);

		Method getMetaInfo = Class.forName(
				"adatao.bigr.spark.execution.LoadTable").getDeclaredMethod(
				"getMetaInfo", JavaRDD.class);
		getMetaInfo.setAccessible(true);
		MetaInfo[] metaInfoArray = (MetaInfo[]) getMetaInfo.invoke(lt
				.setFileURL(fileURL).setHasHeader(false).setSeparator(","),
				fileRDD);
		System.out.println(Arrays.toString(metaInfoArray));

		Field bcMetaInfo = Class.forName(
				"adatao.bigr.spark.execution.LoadTable").getDeclaredField(
				"broadcastMetaInfo");
		bcMetaInfo.setAccessible(true);
		bcMetaInfo.set(lt, sc.broadcast(metaInfoArray));

		Method getDataFrame = Class.forName(
				"adatao.bigr.spark.execution.LoadTable").getDeclaredMethod(
				"getDataFrame", JavaRDD.class);
		getDataFrame.setAccessible(true);
		@SuppressWarnings("unchecked")
		JavaRDD<Object[]> dataFrame = (JavaRDD<Object[]>) getDataFrame.invoke(
				lt, fileRDD);

		System.out.println("Count = " + dataFrame.cache().count());
		Thread.sleep(100000);

		System.out.println("Time = " + (System.currentTimeMillis() - time)
				+ "ms");
	}
}
