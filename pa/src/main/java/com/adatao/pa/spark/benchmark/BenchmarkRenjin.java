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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.StringArrayVector;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

@SuppressWarnings("serial")
public class BenchmarkRenjin implements Serializable {

	private static final long serialVersionUID = 1L;
	static long totalTime = 0;
	public static long total = 0;
	ScriptEngine engine = null;
	private static Object sEngineLock = new Object();

	public void writeToFile(String fileName, String data) throws IOException {
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

	public synchronized ScriptEngine newScriptEngine() {
		synchronized (sEngineLock) {
			// return new ScriptEngineManager().getEngineByName("Renjin");
			return new RenjinScriptEngineFactory().getScriptEngine();
		}
	}

	public Object evalRenjin(String s, final String flatmap, final String map,
			final String reduce) throws ScriptException {
		try {
			if (engine == null) {
				engine = newScriptEngine();
				if (flatmap != null)
					engine.eval("f<-" + flatmap);
				if (map != null)
					engine.eval("m<-" + map);
				if (reduce != null)
					engine.eval("r<-" + reduce);
			}
			return engine.eval(s);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error command = " + s);
			System.err.println("Engine = " + engine);
			return null;
		}
	}

	@SuppressWarnings("deprecation")
	public void runRenjin()  {
		final String flatmap = "function(s) { strsplit(s, split=',')[[1]] }";
		final String m = "function(key) { list(key=key, val=1) }";
		final String reduce = "function(val1, val2) { val1+val2 }";
		String logFile = "hdfs://10.170.9.131:9000/airline-big.csv";
		JavaSparkContext sc = new JavaSparkContext("mesos://10.170.9.131:5050",
				"Renjin " + new Date().toGMTString(), "/root/spark",
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
							StringArrayVector ss = (StringArrayVector) evalRenjin(
									"f('" + s.replace("'", "") + "')", flatmap,
									m, reduce);
							for (int i = 0; i < ss.length(); i++) {
								String word = ss.getElementAsString(i);
								if (map.containsKey(word))
									map.put(word, map.get(word) + 1);
								else
									map.put(word, 1);
							}
						}
						List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
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
					public Integer call(Integer i1, Integer i2)
							throws Exception {
						Object res = evalRenjin("r(" + i1 + "," + i2 + ")",
								flatmap, m, reduce);
						DoubleArrayVector dav = (DoubleArrayVector) res;
						return (int) Math.round(dav.getElementAsDouble(0));
					}
				});
		try {
			writeToFile("output.txt", counts.collectAsMap().toString());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public void runRenjinWithClassicReducer() {
		final String flatmap = "function(s) { strsplit(s, split=',')[[1]] }";
		final String m = "function(key) { list(key=key, val=1) }";
		final String reduce = "function(key, vals) { sum(vals) }";
		String logFile = "hdfs://10.170.11.185:9000/airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext(
				"mesos://10.170.11.185:5050", "Renjin "
						+ new Date().toGMTString(), "/root/spark",
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
							StringArrayVector ss = (StringArrayVector) evalRenjin(
									"f('" + s.replace("'", "") + "')", flatmap,
									m, reduce);
							for (int i = 0; i < ss.length(); i++) {
								String word = ss.getElementAsString(i);
								if (map.containsKey(word))
									map.put(word, map.get(word) + 1);
								else
									map.put(word, 1);
							}
						}
						List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
						Iterator<String> ite = map.keySet().iterator();
						while (ite.hasNext()) {
							String key = ite.next();
							Integer val = map.get(key);
							res.add(new Tuple2<String, Integer>(key, val));
						}
						return res;
					}
				});

		// Step 2
		JavaPairRDD<String, List<Integer>> twos = ones.groupByKey();
		JavaPairRDD<String, Integer> counts = twos
				.map(new PairFunction<Tuple2<String, List<Integer>>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(
							Tuple2<String, List<Integer>> t) throws Exception {
						if (t._2.size() == 0)
							return new Tuple2<String, Integer>(t._1(), 0);
						StringBuffer sb = new StringBuffer();
						sb.append("r('" + t._1() + "', c(");
						for (int val : t._2())
							sb.append(val + ",");
						Object res = evalRenjin(
								sb.substring(0, sb.length() - 1) + "))",
								flatmap, m, reduce);
						DoubleArrayVector dav = (DoubleArrayVector) res;
						return new Tuple2<String, Integer>(t._1(), (int) Math
								.round(dav.getElementAsDouble(0)));
					}
				});
		try {
			writeToFile("output.txt", counts.collectAsMap().toString());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public void runRenjinLocal() {
		final String flatmap = "function(s) { strsplit(s, split=',')[[1]] }";
		final String m = "function(key) { list(key=key, val=1) }";
		final String reduce = "function(val1, val2) { val1+val2 }";
		String logFile = "airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext("local", "Renjin "
				+ new Date().toGMTString());
		JavaRDD<String> lines = sc.textFile(logFile);
		JavaPairRDD<String, Integer> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
					@Override
					public Iterable<Tuple2<String, Integer>> call(
							Iterator<String> t) throws Exception {
						HashMap<String, Integer> map = new HashMap<String, Integer>();
						while (t.hasNext()) {
							String s = t.next();
							StringArrayVector ss = (StringArrayVector) evalRenjin(
									"f('" + s.replace("'", "\'") + "')",
									flatmap, m, reduce);
							if (ss == null)
								continue;
							for (int i = 0; i < ss.length(); i++) {
								String word = ss.getElementAsString(i);
								if (map.containsKey(word))
									map.put(word, map.get(word) + 1);
								else
									map.put(word, 1);
							}
						}
						List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
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

					public Integer call(Integer i1, Integer i2)
							throws Exception {
						Object res = evalRenjin("r(" + i1 + "," + i2 + ")",
								flatmap, m, reduce);
						if (res == null)
							return 0;
						DoubleArrayVector dav = (DoubleArrayVector) res;
						return (int) Math.round(dav.getElementAsDouble(0));
					}
				});
		try {
			writeToFile("output.txt", counts.collectAsMap().toString());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		long time = System.currentTimeMillis();
		new BenchmarkRenjin().runRenjinWithClassicReducer();
		System.out.println("RTime = " + totalTime + "ms");
		System.out.println("Time = " + (System.currentTimeMillis() - time)
				+ "ms");
	}
}
