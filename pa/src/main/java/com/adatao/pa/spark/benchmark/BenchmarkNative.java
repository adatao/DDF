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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

@SuppressWarnings("serial")
public class BenchmarkNative {

	public static void runNative() throws Exception {
		String logFile = "airline-tiny.csv";
		JavaSparkContext sc = new JavaSparkContext("local", "Wordcount");
		JavaRDD<String> lines = sc.textFile(logFile).cache();
		JavaPairRDD<String, Double> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, Double>() {
					@Override
					public Iterable<Tuple2<String, Double>> call(
							Iterator<String> t) throws Exception {
						ArrayList<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
						while (t.hasNext()) {
							String s = t.next();
							String[] ss = s.split(",");
							for (int i = 0; i < ss.length; i++)
								res.add(new Tuple2<String, Double>(ss[i], 1.0));
						}
						return res;
					}
				});

		JavaPairRDD<String, List<Double>> twos = ones.groupByKey();
		JavaPairRDD<String, Double> counts = twos
				.map(new PairFunction<Tuple2<String, List<Double>>, String, Double>() {
					@Override
					public Tuple2<String, Double> call(
							Tuple2<String, List<Double>> t) throws Exception {
						double res = 0.0;
						for (Double val : t._2())
							res += val;
						return new Tuple2<String, Double>(t._1, res);
					}
				});
		System.out.println(counts.collectAsMap());
	}

	public static void runNativeOptimize() throws Exception {
		String logFile = "hdfs://smaster:9000/airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext("mesos://smaster:5050",
				"Wordcount", "/root/spark",
				new String[] { "./target/rserver-0.1.0.jar" });
		JavaRDD<String> lines = sc.textFile(logFile);
		JavaPairRDD<String, Integer> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
					@Override
					public Iterable<Tuple2<String, Integer>> call(
							Iterator<String> t) throws Exception {
						HashMap<String, Integer> map = new HashMap<String, Integer>();
						while (t.hasNext()) {
							String s = t.next();
							String[] ss = s.split(",");
							for (int i = 0; i < ss.length; i++)
								if (map.containsKey(ss[i]))
									map.put(ss[i], map.get(ss[i]) + 1);
								else
									map.put(ss[i], 1);
						}
						ArrayList<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
						Iterator<String> ite = map.keySet().iterator();
						while (ite.hasNext()) {
							String key = ite.next();
							Integer val = map.get(key);
							res.add(new Tuple2<String, Integer>(key, val));
						}
						return res;
					}
				});
		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		counts.saveAsTextFile("hdfs://smaster:9000/output");
		// writeToFile("output.txt", counts.collectAsMap().toString());
	}

	public static void writeToFile(String fileName, String data)
			throws IOException {
		if (fileName.lastIndexOf("/") > 0) {
			String dirPath = fileName.substring(0, fileName.lastIndexOf("/"));
			File dir = new File(dirPath);
			if (!dir.exists())
				dir.mkdirs();
		}
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(fileName), "UTF8"));
		out.write(data);
		out.close();
	}

	public static void runNativeWithClassicReduce() throws Exception {
		String logFile = "hdfs://10.29.198.154:9000/airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext(
				"mesos://10.29.198.154:5050", "Wordcount", "/root/spark",
				new String[] { "./target/rserver-0.1.0.jar" });
		JavaRDD<String> lines = sc.textFile(logFile);
		JavaPairRDD<String, Integer> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
					@Override
					public Iterable<Tuple2<String, Integer>> call(
							Iterator<String> t) throws Exception {
						HashMap<String, Integer> map = new HashMap<String, Integer>();
						while (t.hasNext()) {
							String s = t.next();
							String[] ss = s.split(",");
							for (int i = 0; i < ss.length; i++)
								if (map.containsKey(ss[i]))
									map.put(ss[i], map.get(ss[i]) + 1);
								else
									map.put(ss[i], 1);
						}
						ArrayList<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
						Iterator<String> ite = map.keySet().iterator();
						while (ite.hasNext()) {
							String key = ite.next();
							Integer val = map.get(key);
							res.add(new Tuple2<String, Integer>(key, val));
						}
						return res;
					}
				});
		JavaPairRDD<String, List<Integer>> twos = ones.groupByKey();
		JavaPairRDD<String, Integer> counts = twos
				.map(new PairFunction<Tuple2<String, List<Integer>>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(
							Tuple2<String, List<Integer>> t) throws Exception {
						int res = 0;
						for (int val : t._2())
							res += val;
						return new Tuple2<String, Integer>(t._1, res);
					}
				});
		writeToFile("output.txt", counts.collectAsMap().toString());
	}

	public static void main(String[] args) throws Exception {
		long time = System.currentTimeMillis();
		runNativeOptimize();
		System.out.println("Time = " + (System.currentTimeMillis() - time)
				+ "ms");
	}
}
