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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.StringArrayVector;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

@SuppressWarnings("serial")
public class MapReduce implements Serializable {

	static MapReduce instance = null;

	public static MapReduce getInstance() {
		if (instance == null)
			instance = new MapReduce();
		return instance;
	}

	static long rtime = 0, count = 0, num = 0;

	private static Object sEngineLock = new Object();
	ScriptEngine engine = null;

	public synchronized ScriptEngine newScriptEngine() {
		synchronized (sEngineLock) {
			// return new ScriptEngineManager().getEngineByName("Renjin");
			return new RenjinScriptEngineFactory().getScriptEngine();
		}
	}

	public Object evalRenjin(String s, final String map, final String reduce, final String combine)
			throws ScriptException {
		return evalRenjin(s, map, reduce, combine, null);
	}

	public Object evalRenjin(String s, final String map, final String reduce, final String combine, final String data)
			throws ScriptException {
		try {
			if (engine == null) {
				engine = newScriptEngine();
				if (data != null && data.length() > 0)
					engine.eval("data<-unserialize(" + data + ")");
				if (map != null && map.length() > 0)
					engine.eval("m<-" + map);
				if (reduce != null && reduce.length() > 0)
					engine.eval("r<-" + reduce);
				if (combine != null && combine.length() > 0)
					engine.eval("co<-" + combine);
			}
			return engine.eval(s);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error command = " + s);
			System.err.println("Engine = " + engine);
			return null;
		}
	}

	public JavaPairRDD<String, String> runMapReduce(JavaRDD<String> lines,
			final String map, final String reduce, final String combine)
			throws ScriptException {

		// Step 1
		// map <- function(key, val) { lapply( strsplit(x=val, split=' ')[[1]],
		// function(w) list(k=w, v=1) ) }
		JavaPairRDD<String, String> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(
							Iterator<String> s) throws Exception {
						List<Tuple2<String, String>> res = new ArrayList<Tuple2<String, String>>();
						try {
							if (combine == null || combine.isEmpty()) {
								while (s.hasNext()) {
									if (count++ % 1000 == 0)
										System.out.println(count);
									String next = s.next();
									long time = System.currentTimeMillis();
									ListVector lv = (ListVector) evalRenjin(
											"m(,'"
													+ next.replace("\"", "\\\"")
															.replace("'", "\\'")
													+ "')", map, reduce,
											combine);
									rtime += (System.currentTimeMillis() - time);
									for (int i = 0; i < lv.length(); i++) {
										ListVector l = (ListVector) lv.get(i);
										res.add(new Tuple2<String, String>(l
												.getElementAsString(0), l
												.getElementAsString(1)));
									}
								}
							}

							// Step 1b: Combine before reduce
							else {
								Map<String, String> m = new HashMap<String, String>();
								while (s.hasNext()) {
									if (count++ % 1000 == 0)
										System.out.println(count);
									String next = s.next();
									long time = System.currentTimeMillis();
									ListVector lv = (ListVector) evalRenjin(
											"m(,'"
													+ next.replace("\"", "\\\"")
															.replace("'", "\\'")
													+ "')", map, reduce,
											combine);
									rtime += (System.currentTimeMillis() - time);
									for (int i = 0; i < lv.length(); i++) {
										ListVector l = (ListVector) lv.get(i);
										String key = l.getElementAsString(0);
										String val = l.getElementAsString(1);
										if (m.containsKey(key)) {
											// Combine
											time = System.currentTimeMillis();
											String newVal = ((StringArrayVector) evalRenjin(
													"co('" + m.get(key) + "','"
															+ val + "')", map,
													reduce, combine))
													.getElementAsString(0)
													.replace(",", "");
											rtime += (System
													.currentTimeMillis() - time);
											m.put(key, newVal);
										} else
											m.put(key, val);
									}
								}
								for (String key : m.keySet())
									res.add(new Tuple2<String, String>(key, m
											.get(key)));
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						return res;
					}
				});

		// Step 2
		// reduce <- function(val1, val2) { val1+val2 }
		JavaPairRDD<String, String> counts = ones
				.reduceByKey(new Function2<String, String, String>() {
					private static final long serialVersionUID = 1L;

					public String call(String i1, String i2) {
						try {
							if (count++ % 1000 == 0)
								System.out.println(count);
							long time = System.currentTimeMillis();
							String res = ((StringArrayVector) evalRenjin("r('"
									+ i1 + "','" + i2 + "')", map, reduce,
									combine)).getElementAsString(0).replace(
									",", "");
							rtime += (System.currentTimeMillis() - time);
							return res;
						} catch (Exception e) {
							e.printStackTrace();
						}
						return "";
					}
				});
		return counts;
	}

	public void checkMapReduceFunc(String map, String reduce, String combine)
			throws ScriptException {
		// ScriptEngine engine = newScriptEngine();
		// if (map != null) {
		// engine.eval("m<-" + map);
		// engine.eval("m(,'test')");
		// }
		// if (reduce != null) {
		// engine.eval("r<-" + reduce);
		// engine.eval("r('test',c('1'))");
		// }
		// if (combine != null) {
		// engine.eval("co<-" + combine);
		// engine.eval("co('test',c('1'))");
		// }
	}

	public JavaPairRDD<String, String> runMapReduceWithHadoopReducer(
			JavaRDD<String> lines, final String map, final String reduce,
			final String combine, final String data) throws ScriptException {
		// Step 1
		// map <- function(key, val) { lapply( strsplit(x=val, split=' ')[[1]],
		// function(w) list(k=w, v=1) ) }
		JavaPairRDD<String, String> ones = lines
				.mapPartitions(new PairFlatMapFunction<Iterator<String>, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(
							Iterator<String> s) throws Exception {
						List<Tuple2<String, String>> res = new ArrayList<Tuple2<String, String>>();
						try {
							if (combine == null || combine.isEmpty()) {
								while (s.hasNext()) {
									if (count++ % 1000 == 0)
										System.out.println(count);
									String next = s.next();
									long time = System.currentTimeMillis();
									ListVector lv = (ListVector) evalRenjin(
											"m(,'"
													+ next.replace("\"", "\\\"")
															.replace("'", "\\'")
													+ "')", map, reduce,
											combine, data);
									rtime += (System.currentTimeMillis() - time);
									for (int i = 0; i < lv.length(); i++) {
										ListVector l = (ListVector) lv.get(i);
										res.add(new Tuple2<String, String>(l
												.getElementAsString(0), l
												.getElementAsString(1)));
									}
								}
							}

							// Step 1b: Combine before reduce
							else {
								Map<String, List<String>> m = new HashMap<String, List<String>>();
								int num = 0;
								while (s.hasNext()) {
									if (count++ % 1000 == 0)
										System.out.println(count);
									String next = s.next();
									long time = System.currentTimeMillis();
									ListVector lv = (ListVector) evalRenjin(
											"m(,'"
													+ next.replace("\"", "\\\"")
															.replace("'", "\\'")
													+ "')", map, reduce,
											combine);
									rtime += (System.currentTimeMillis() - time);
									for (int i = 0; i < lv.length(); i++) {
										ListVector l = (ListVector) lv.get(i);
										String key = l.getElementAsString(0);
										String val = l.getElementAsString(1);
										if (m.containsKey(key)) {
											m.get(key).add(val);
										} else {
											List<String> list = new ArrayList<String>();
											list.add(val);
											m.put(key, list);
										}
									}
									num++;
									if (num == 10000) {
										// Combine
										for (String key : m.keySet()) {
											StringBuffer sb = new StringBuffer();
											List<String> vals = m.get(key);
											sb.append("co('" + key + "', c(");
											for (String val : vals)
												sb.append("'" + val + "',");
											String val1 = ((StringArrayVector) evalRenjin(
													sb.substring(0,
															sb.length() - 1)
															+ "))", map,
													reduce, combine))
													.getElementAsString(0)
													.replace(",", "");
											res.add(new Tuple2<String, String>(
													key, val1));
										}
										m.clear();
										num = 0;
									}
								}
								// Combine
								if (num > 0)
									for (String key : m.keySet()) {
										StringBuffer sb = new StringBuffer();
										List<String> vals = m.get(key);
										sb.append("co('" + key + "', c(");
										for (String val : vals)
											sb.append("'" + val + "',");
										String val1 = ((StringArrayVector) evalRenjin(
												sb.substring(0, sb.length() - 1)
														+ "))", map, reduce,
												combine)).getElementAsString(0)
												.replace(",", "");
										res.add(new Tuple2<String, String>(key,
												val1));
									}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						return res;
					}
				});

		JavaPairRDD<String, List<String>> twos = ones.groupByKey();

		// Step 2
		// reduce <- function(key, vals) { ... }
		JavaPairRDD<String, String> counts = twos
				.map(new PairFunction<Tuple2<String, List<String>>, String, String>() {
					@Override
					public Tuple2<String, String> call(
							Tuple2<String, List<String>> t) throws Exception {
						try {
							StringBuffer sb = new StringBuffer();
							sb.append("r('" + t._1() + "', c(");
							for (String val : t._2())
								sb.append("'" + val + "',");
							String res = ((StringArrayVector) evalRenjin(
									sb.substring(0, sb.length() - 1) + "))",
									map, reduce, combine))
									.getElementAsString(0).replace(",", "");
							return new Tuple2<String, String>(t._1(), res);
						} catch (Exception e) {
							return new Tuple2<String, String>(t._1(), "");
						}
					}
				});
		return counts;
	}

	public void testMapReduce() throws Exception {
		// String logFile = "airline-tiny.csv";
		// JavaSparkContext sc = new JavaSparkContext("local",
		// "Test MapReduce1");
		String logFile = "hdfs://10.170.26.203:9000/airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext(
				"mesos://10.170.26.203:5050", "Wordcount", "/root/spark",
				new String[] { "./target/rserver-0.1.0.jar" });
		JavaRDD<String> lines = sc.textFile(logFile);
		JavaPairRDD<String, String> counts = runMapReduce(
				lines,
				"function(k,v) { lapply( strsplit(x=v, split=',')[[1]], function(w) list(k=w, v=1) ) }",
				"function(v1, v2) { as.character(as.integer(v1) + as.integer(v2)) }",
				"function(v1, v2) { as.character(as.integer(v1) + as.integer(v2)) }");
		System.out.println(counts.collectAsMap());
	}

	public void testMapReduceWithHadoopReducer() throws Exception {
		// String logFile = "airline-small.csv";
		// JavaSparkContext sc = new JavaSparkContext("local",
		// "MapReduce With Hadoop Reducer");
		String logFile = "hdfs://10.170.26.203:9000/airline-medium.csv";
		JavaSparkContext sc = new JavaSparkContext(
				"mesos://10.170.26.203:5050", "MapReduce With Hadoop Reducer",
				"/root/spark", new String[] { "./target/rserver-0.1.0.jar" });
		JavaRDD<String> lines = sc.textFile(logFile);
		JavaPairRDD<String, String> counts = runMapReduceWithHadoopReducer(
				lines,
				"function(k,v) { lapply( strsplit(x=v, split=',')[[1]], function(w) list(k=w, v=1) ) }",
				"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
				"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
				null);
		counts.collectAsMap();
	}

	public static void main(String[] args) throws Exception {
		long time = System.currentTimeMillis();

		MapReduce.getInstance().testMapReduceWithHadoopReducer();

		System.out.println("RTime = " + rtime + "ms");
		System.out.println("Time = " + (System.currentTimeMillis() - time)
				+ "ms");
	}
}
