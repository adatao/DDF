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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import com.adatao.pa.spark.execution.DDFExecutor;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shark.SharkEnv;
import shark.api.JavaSharkContext;
import io.ddf.DDFManager;
import io.spark.ddf.SparkDDFManager;
import io.ddf.content.ViewHandler.Expression;
import io.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.execution.ExecutionContext;
import com.adatao.pa.spark.execution.Subset;
import com.adatao.pa.spark.execution.TExecutor;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.spark.types.FailedResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.types.ASessionThread;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class SparkThread extends ASessionThread {

	public static Logger LOG = LoggerFactory.getLogger(SparkThread.class);

	ArrayBlockingQueue<Object> cmdQueue;
	ArrayBlockingQueue<Object> resQueue;

	Date latestCommandTime;

	JavaSparkContext sparkContext;
	DataManager dataManager = new DataManager();
	DDFManager ddfManager;

	int driverPort = 20002;
	int uiPort = 30001;

	GsonBuilder gsonBld = new GsonBuilder().serializeSpecialFloatingPointValues()
							.registerTypeAdapter(Expression.class, new Subset.ExpressionDeserializer());
	Gson gson = gsonBld.setExclusionStrategies(new ExclusionStrategy() {
		@Override
		public boolean shouldSkipField(FieldAttributes arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		/**
		 * We need to ignore Manifest type that occur in Scala classes
		 * (e.g., Manifest[LinearRegressionModel]), and we don't know
		 * how to/don't need to deserialize them either for our purposes
		 * here.
		 */
		@Override
		public boolean shouldSkipClass(Class<?> arg0) {
			if (scala.reflect.Manifest.class.equals(arg0))
				return true;
			return false;
		}
	}).create();
	
	public void stopMe(){
		ddfManager.shutdown();
		this.interrupt();
	}
	
	public SparkThread(ArrayBlockingQueue<Object> cmdQueue, ArrayBlockingQueue<Object> resQueue) {
		this.cmdQueue = cmdQueue;
		this.resQueue = resQueue;
	}

	@SuppressWarnings("unused")
	private void processJsonCommand(JsonCommand jsCmd) throws JsonSyntaxException, InterruptedException, ClassNotFoundException, AdataoException {
			Object exec = gson.fromJson(jsCmd.params, Class.forName("com.adatao.pa.spark.execution." + jsCmd.getCmdName()));
			LOG.info("Created Executor: " + exec.toString());

			ExecutionResult<?> execRes = null;

			if (exec instanceof IExecutor) {
				// Old-style, Java-based class hierarchy
				// Note: this means every old ExecutorResult will be WRAPPED
				// within an upon return ExecutionResult.
				// But based on our ExecutionResult scheme, this is as all
				// clients should expect. It's just that the
				// "success" field may annoyingly appear twice: once at the top
				// level, and once within the "result" object.
				execRes = ExecutionResult.newInstance(((IExecutor) exec).run(this));
			} else if (exec instanceof TExecutor) {
				// New-style, Scala-based class hierarchy
				execRes = ((TExecutor<?>) exec).run(new ExecutionContext(this));
			}
			resQueue.put(new JsonResult().setResult(execRes.toJson()));
	}
	
	public ExecutionResult<?> processJsonCommand1(JsonCommand jsCmd) throws JsonSyntaxException, ClassNotFoundException, AdataoException {
			Object exec = gson.fromJson(jsCmd.params, Class.forName("com.adatao.pa.spark.execution." + jsCmd.getCmdName()));
			LOG.info("Created Executor: " + exec.toString());

			ExecutionResult<?> execRes = null;
			if (exec instanceof IExecutor) {
				// Old-style, Java-based class hierarchy
				// Note: this means every old ExecutorResult will be WRAPPED
				// within an upon return ExecutionResult.
				// But based on our ExecutionResult scheme, this is as all
				// clients should expect. It's just that the
				// "success" field may annoyingly appear twice: once at the top
				// level, and once within the "result" object.
				execRes = ExecutionResult.newInstance(((IExecutor) exec).run(this));
			} else if (exec instanceof TExecutor) {
				// New-style, Scala-based class hierarchy
				execRes = ((TExecutor<?>) exec).run(new ExecutionContext(this));
			} else if (exec instanceof DDFExecutor) {
        execRes = ((DDFExecutor) exec).run(new ExecutionContext(this));
      }
			
			return execRes;
	}

//	private void deleteTempHiveTables() {
//		HashMap<String, DataContainer> dcMap = dataManager.getDataContainers();
//		for (DataContainer dc : dcMap.values()) {
//			if (dc.getType() == DataContainer.ContainerType.SharkDataFrame) {
//				((JavaSharkContext) sparkContext).sql2console("drop table if exists " + ((SharkDataFrame) dc).getTableName());
//			}
//		}
//	}

	private static Map<String, String> getEnvironment() {
		Map<String, String> result = new HashMap<String, String>();

		String s = null;

		s = System.getProperty("spark.classpath", System.getenv("SPARK_CLASSPATH"));
		if (s != null && s.length() > 0)
			result.put("SPARK_CLASSPATH", s);

		s = System.getProperty("spark.master", System.getenv("SPARK_MASTER"));
		if (s != null && s.length() > 0)
			result.put("SPARK_MASTER", s);
		else {
			try {
				result.put("SPARK_MASTER", String.format("spark://%s:7077", InetAddress.getLocalHost().getHostName()));
			} catch (Exception e) {
				// Ignore
			}
		}

		s = System.getProperty("spark.home", System.getenv("SPARK_HOME"));
		if (s != null && s.length() > 0)
			result.put("SPARK_HOME", s.replaceAll(" ", "%20"));

		s = System.getProperty("rserver.home", System.getenv("RSERVER_HOME"));
		if (s != null && s.length() > 0)
			result.put("RSERVER_HOME", s.replaceAll(" ", "%20"));

		s = System.getProperty("rserver.jar", System.getenv("RSERVER_JAR"));
		if (s != null && s.length() > 0)
			result.put("RSERVER_JAR", s.replaceAll(" ", "%20"));
		else if (result.containsKey("RSERVER_HOME")) {
			// Desperate times call for desperate measures
			String rserverJar = result.get("RSERVER_HOME") + "/target/scala-2.9.2/bigr_server_2.9.2-0.1.jar";
			result.put("RSERVER_JAR", rserverJar.replaceAll(" ", "%20"));
		}

		// Apply some requirements
		if (!result.containsKey("SPARK_MASTER") || !result.containsKey("SPARK_HOME") || !result.containsKey("RSERVER_JAR")) {
			LOG.warn("SPARK_MASTER or SPARK_HOME or RSERVER_JAR/RSERVER_HOME not defined");
			System.exit(1);
		}

		// Debug print out the server environment
		LOG.info(SparkThread.class.getSimpleName() + ": Starting up with these environment settings:");
		for (String name : new String[] { "SPARK_HOME", "SPARK_MASTER", "SPARK_CLASSPATH", "RSERVER_JAR" }) {
			LOG.info(String.format("%s=%s", name, result.get(name)));
		}

		return result;
	}

	// @SuppressWarnings("rawtypes")
	public JavaSparkContext startSparkContext(Boolean isShark) throws IOException, FileNotFoundException, DDFException {
		Map<String, String> env = SparkThread.getEnvironment();
		String[] jobJars = env.get("RSERVER_JAR").split(",");

		System.setProperty("spark.driver.port", Integer.toString(driverPort));
		System.setProperty("spark.ui.port", Integer.toString(uiPort));

		JavaSparkContext sc = null;
		
		ddfManager = DDFManager.get("spark");
		
		return sc;
	}

	public JavaSparkContext startLocalSparkContext(Boolean isShark) {
		if (!isShark)
			return new JavaSparkContext("local[2]", "BigR");
		else
			return SharkEnv.initWithJavaSharkContext(new JavaSharkContext("local[2]", "BigR"));
	}

	public void run() {
		LOG.info("Starting SparkThread ...");

		// Use SPARK_MODE to determine whether to run on Spark cluster or local
		String sparkMode = System.getenv("SPARK_MODE");

		try {
			if (sparkMode != null && sparkMode.toLowerCase().trim().equals("local")) {
				sparkContext = startLocalSparkContext(isShark);
			} else {
				sparkContext = startSparkContext(isShark);
			}
		} catch (Exception e) {
			LOG.error("Exception while starting SharkContext: ", e);
			LOG.error(AdataoExceptionCode.ERR_GENERAL.name());
			JsonResult res = new JsonResult().setResult(new FailedResult<Object>(AdataoExceptionCode.ERR_GENERAL.getMessage()).toJson());
			try {
				resQueue.put(res);
			} catch (InterruptedException e1){
				LOG.info("Thread is interrupted. Failed to send back result!");
			}
			return;
		}
		
		if (sessionID != null) {
			// if the sessionID has been given then use it
			// this is the case when SparkThread is started by a Worker in MultiUser/MultiContext mode
			// and the sessionID is given by Server
			
			//thriftPort is not used here so set it to 0
			sessionManager.addSession(this, sessionID, clientID, 0, uiPort, driverPort);
		} else {
			//thriftPort is not used here so set it to 0
			sessionID = sessionManager.addSession(this, clientID, 0, uiPort, driverPort).sessionID();
		}
		try {
			resQueue.put(new JsonResult().setSid(sessionID));
		} catch (InterruptedException e){
			LOG.info("Thread is interrupted. Failed to send back result!");
		}
	}

	public DDFManager getDDFManager() {
	  return ddfManager;
	}
	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public ArrayBlockingQueue<Object> getCommandQueue() {
		return cmdQueue;
	}

	public ArrayBlockingQueue<Object> getResultQueue() {
		return resQueue;
	}

	public DataManager getDataManager() {
		return dataManager;
	}

	public Date getLatestCommandTime() {
		return latestCommandTime;
	}

	public void setLatestCommandTime(Date latestCommandTime) {
		this.latestCommandTime = latestCommandTime;
	}

	public SparkThread setShark(boolean isShark) {
		this.isShark = isShark;
		return this;
	}

	public SparkThread setDriverPort(int driverPort) {
		this.driverPort = driverPort;
		return this;
	}

	public SparkThread setUiPort(int uiPort) {
		this.uiPort = uiPort;
		return this;
	}

	public SparkThread setCmdQueue(ArrayBlockingQueue<Object> cmdQueue) {
		this.cmdQueue = cmdQueue;
		return this;
	}

	public SparkThread setResQueue(ArrayBlockingQueue<Object> resQueue) {
		this.resQueue = resQueue;
		return this;
	}
}
