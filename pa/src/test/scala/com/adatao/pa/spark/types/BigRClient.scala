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

/**
 *
 */
package com.adatao.pa.spark.types

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.gson.Gson
import com.adatao.pa.thrift.Server
import com.adatao.pa.thrift.generated.JsonCommand
import com.adatao.pa.thrift.generated.RCommands
import com.adatao.pa.thrift.generated.JsonResult
import com.google.gson.GsonBuilder
//import com.adatao.pa.spark.MultiContextConnectResult
import com.adatao.pa.spark.execution.LoadTable
import com.adatao.pa.spark.execution.Sql2DataFrame
import com.adatao.pa.spark.execution.Sql2ListString
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.pa.spark.execution.Sql2ListString.Sql2ListStringResult
import org.json.JSONObject
import java.util.ArrayList
import org.apache.commons.lang.StringUtils
import com.adatao.pa.spark.execution.Subset.SubsetResult

/**
 * Simulates an RClient communicating with BigR/server via Thrift
 *
 * @author ctn
 *
 */
class BigRClient(serverHost: String, serverPort: Int) {
	private val LOG: Logger = LoggerFactory.getLogger(this.getClass())

	private var sid: String = null
	private var workerPort: Int = 0
	private var workerHost: String = null
	//	private var thriftTransport: TSocket = null
	private var thriftClienttoServer: RCommands.Client = null
	private var thriftClienttoWorker: RCommands.Client = null

	if (System.getProperty("bigr.multiuser", "true").toBoolean) {
		val thriftTransport = new TSocket(serverHost, serverPort);
		thriftTransport.open
		val protocol = new TBinaryProtocol(thriftTransport)
		thriftClienttoServer = new RCommands.Client(protocol)
		// thriftClienttoWorker will be created after connect
	}
	else {
		val thriftTransport = new TSocket(serverHost, serverPort);
		thriftTransport.open
		val protocol = new TBinaryProtocol(thriftTransport)
		thriftClienttoServer = new RCommands.Client(protocol)
		thriftClienttoWorker = thriftClienttoServer
	}

	/**
	 * Update thrift client to connect to the right host and port
	 */
	def updateThriftClientWorker() = {
		if (thriftClienttoWorker != null)
			thriftClienttoWorker.getOutputProtocol().getTransport().close()

		val thriftTransport = new TSocket(workerHost, workerPort);
		thriftTransport.open
		val protocol = new TBinaryProtocol(thriftTransport)
		thriftClienttoWorker = new RCommands.Client(protocol)
	}

	/**
	 * Connects to the BigR server
	 */
	def connect(params: String = null) = {
		if (System.getProperty("bigr.multiuser", Server.MULTIUSER_DEFAULT.toString).toBoolean) {
/*			val res = this.execute[MultiContextConnectResult](thriftClienttoServer, "connect", params)
			sid = res.result.sessionID
			workerPort = res.result.thriftPort
			workerHost = res.result.host
			LOG.info("connect: SID=%s host=%s thriftPort=%d".format(sid, workerHost, workerPort))
			// connect to worker now
			//			Thread.sleep(5000)
			updateThriftClientWorker()*/
		}
		else {
			sid = this.executeImpl(thriftClienttoServer, "connect", params).sid
			LOG.info("connect: SID=%s".format(sid))
		}
	}

	/**
	 * Disconnects from the BigR server
	 */
	def disconnect = {
		LOG.info("disconnect: %s".format(this.executeImpl(thriftClienttoServer, "disconnect", null).toString))
	}

	/**
	 * Executes a command given a command object with its parameters already set, and which will be serialized
	 * before being sent to the BigR Thrift server
	 *
	 * @return ExecutionResult[T]
	 */
	def execute[T](commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] =
		execute[T](thriftClienttoWorker, commandObject)

	def execute[T](thriftClient: RCommands.Client, commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] = {
		this.execute[T](thriftClient, commandObject.getClass.getSimpleName, new Gson().toJson(commandObject))
	}

	def executeNew[T](commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] =
		executeNew[T](thriftClienttoWorker, commandObject)

	def executeNew[T](thriftClient: RCommands.Client, commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] = {

		var b = new JSONObject(new Gson().toJson(commandObject))
		b.put("methodName", commandObject.getClass.getSimpleName)
		b.put("ddfUri", commandObject.getClass.getSimpleName)
		println(">>>newjson b: " + b)
		this.execute[T](thriftClient, "DDFExecutor", b.toString())
		
//		this.execute[T](thriftClient, commandObject.getClass.getSimpleName, new Gson().toJson(commandObject))
		
	}

	/**
	 * Executes a command string together with supplied parameters. Note that this is NOT
	 * compatible with calling [[CExecutor]]s, since they do not return an [[ExecutionResult]] object.
	 * An exception will be thrown when attempting to deserialize results from [[CExecutor]]s.
	 *
	 * The recommended way of working with old [[CExecutor]] is to call executeNoWrap().
	 *
	 * @return ExecutionResult[T]
	 */
	def execute[T](commandName: String, params: String)(implicit m: Manifest[T]): ExecutionResult[T] =
		execute[T](thriftClienttoWorker, commandName, params)

	def execute[T](thriftClient: RCommands.Client, commandName: String, params: String)(implicit m: Manifest[T]): ExecutionResult[T] = {
		val resultJson = this.executeImpl(thriftClient, commandName, params).getResult

		if (false && classOf[ExecutorResult].isAssignableFrom(m.erasure)) {
			// This is expecting a result from the old, deprecated CExecutor
			ExecutionResult.fromJson[T](resultJson) match {
				case sr: SuccessResult ⇒ new SuccessfulResult(sr.asInstanceOf[T])
				case fr: FailResult ⇒ new FailedResult(fr.message)
				case _ ⇒ new FailedResult("Unable to parse result JSON: %s".format(resultJson))
			}
		}
		else {
			// New-style AExecutor already returns result wrapped inside an ExecutionResult (either SuccessfulResult or FailedResult)
			ExecutionResult.fromJson[T](resultJson)
		}
	}

	private def executeImpl(commandName: String, params: String): JsonResult =
		executeImpl(thriftClienttoWorker, commandName, params)

	private def executeImpl(thriftClient: RCommands.Client, commandName: String, params: String): JsonResult = {
		println(">>>>>>>oldparams: " + params)
		val json = thriftClient.execJsonCommand(new JsonCommand().setCmdName(commandName).setSid(sid).setParams(params))
		if (json != null && json.getResult != null && json.getResult.matches(".*\"success\":false.*")) throw new Exception(json.getResult)
		json
	}

	//new DDFExecutor, call DDFExecutor instead of old executor
	private def executeImplnew(thriftClient: RCommands.Client, commandName: String, params: String): JsonResult = {
		val json = thriftClient.execJsonCommand(new JsonCommand().setCmdName("DDFExecutor").setSid(sid).setParams(params))
		if (json != null && json.getResult != null && json.getResult.matches(".*\"success\":false.*")) throw new Exception(json.getResult)
		json
	}
}

/**
 * Companion object/singleton
 */
object BigRClient {

}

object BigRThriftServerUtils {
	var thriftServer: Server = null
	//	var thriftTransport: TTransport = null
	val HOST = "localhost"
	val PORT = 7912

	def startServer: BigRClient = {
		thriftServer = new Server(PORT);

		new Thread(new Runnable() { def run = thriftServer.start }).start;
		Thread.sleep(5000)
		try {
			new BigRClient(HOST, PORT)
		}
		catch {
			case e: TTransportException ⇒ {
				e.printStackTrace
				assert(false)
				null
			}
		}
	}

	def stopServer = {
		thriftServer.stop;
		//		thriftTransport.close
	}

	def createClient: BigRClient = {
		new BigRClient(HOST, PORT)
	}
}

object BigRClientTestUtils {
	/**
	 * Load one of the file paths given and returns the first successful load
	 *
	 * @param fileUrls list of file URLs to try one after another, until successful
	 */
	def loadFile(bigRClient: BigRClient, fileUrls: List[String], hasHeader: Boolean, fieldSeparator: String, sampleSize: Int): String = {
		fileUrls.foreach {
			fileUrl ⇒
				try {
					return this.loadFile(bigRClient, fileUrl, hasHeader, fieldSeparator, sampleSize)
				}
				catch {
					case e: Exception ⇒ println("Ignored Exception: %s".format(e.getMessage()))
				}
		}
		null
	}

	/**
	 * Connects to an existing session and loads the given fileUrl
	 *
	 * @param sessionId
	 * @param fileUrl
	 * @return dataContainerId
	 */
	def loadFile(bigRClient: BigRClient, fileUrl: String, hasHeader: Boolean, fieldSeparator: String, sampleSize: Int): String = {
		bigRClient.execute[LoadTable.LoadTableResult](
		  new LoadTable().setFileURL(fileUrl).setHasHeader(hasHeader).setSeparator(fieldSeparator).setSampleSize(sampleSize)).result.getDataContainerID
	}
  def createTableSample(bigRClient: BigRClient) = {
    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
    assert(runSQLCmd(bigRClient, "drop table if exists stable").isSuccess);
    assert(runSQLCmd(bigRClient, "CREATE TABLE stable (v1 double, v2 string, v3 int, v4 string, v5 string, v6 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/table_noheader_noNA.csv' INTO TABLE stable").isSuccess);
  }
	
	def runSQLCmd(bigRClient: BigRClient, cmdStr: String): Sql2ListStringResult = {
		val sql: Sql2ListString = new Sql2ListString().setSqlCmd(cmdStr)
		bigRClient.execute[Sql2ListStringResult](sql).result
	}

	def runSQL2RDDCmd(bigRClient: BigRClient, cmdStr: String, cache: Boolean): Sql2DataFrameResult = {
		val sql: Sql2DataFrame = new Sql2DataFrame(cmdStr, cache)
		bigRClient.execute[Sql2DataFrameResult](sql).result
	}
	def createTableKmeans(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
		assert(runSQLCmd(bigRClient, "drop table if exists kmeans").isSuccess);
		assert(runSQLCmd(bigRClient, "CREATE TABLE kmeans (v1 double, v2 double ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
		assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/KmeansTest.csv' INTO TABLE kmeans").isSuccess);
	}

//  def createTableGraph(bigRClient: BigRClient) = {
//    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
//    assert(runSQLCmd(bigRClient, "drop table if exists graph").isSuccess);
//    assert(runSQLCmd(bigRClient, "CREATE TABLE graph (source string, dest string, id double, cnt double, dn_cnt double, " +
//      "opp_users double, tf double, idf double, tfidf double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
//    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/graph2.csv' INTO TABLE graph").isSuccess);
//  }

  def createTableGraph(bigRClient: BigRClient) = {
    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
    assert(runSQLCmd(bigRClient, "drop table if exists graph").isSuccess);
    assert(runSQLCmd(bigRClient, "CREATE TABLE graph (source bigint, dest bigint, id double, cnt double, dn_cnt double," +
      "opp_users double, tf double, idf double, tfidf double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/graphNumeric2.csv' INTO TABLE graph").isSuccess);
  }

  def createTableGraph1(bigRClient: BigRClient) = {
    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
    assert(runSQLCmd(bigRClient, "drop table if exists graph1").isSuccess);
    assert(runSQLCmd(bigRClient, "CREATE TABLE graph1 (source string, dest string)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/graph3.csv' INTO TABLE graph1").isSuccess);
  }

  def createTableGraph2(bigRClient: BigRClient) = {
    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
    assert(runSQLCmd(bigRClient, "drop table if exists graph2").isSuccess);
    assert(runSQLCmd(bigRClient, "CREATE TABLE graph2 (source string, dest string)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess);
    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/graph4.csv' INTO TABLE graph2").isSuccess);
  }

	def createTableMtcars(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
		assert(runSQLCmd(bigRClient, "drop table if exists mtcars").isSuccess);
		assert(runSQLCmd(bigRClient, "CREATE TABLE mtcars ("
			+ "mpg double, cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
			+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '").isSuccess);
		assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/mtcars' INTO TABLE mtcars").isSuccess);
	}
	
	 def createTableCarowner(bigRClient: BigRClient) = {
    assert(runSQLCmd(bigRClient, "set shark.test.data.path=" + "resources").isSuccess);
    assert(runSQLCmd(bigRClient, "drop table if exists carowner").isSuccess);
    assert(runSQLCmd(bigRClient, "CREATE TABLE carowner (name string, cyl int, disp double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '").isSuccess());
    assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/carowner' INTO TABLE carowner").isSuccess);
  }

	def createTableAdmission(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists admission").isSuccess)
		assert(runSQLCmd(bigRClient, "create table admission (v1 int, v2 int, v3 double, v4 int)" +
			" row format delimited fields terminated by ' '").isSuccess)
		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/admission.csv' " +
			"INTO TABLE admission").isSuccess)
	}
	
	def createTableAdmission2(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists admission").isSuccess)
		assert(runSQLCmd(bigRClient, "create table admission (v1 int, v2 int, v3 double, v4 int)" +
			" row format delimited fields terminated by ' '").isSuccess)
		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/admission2.csv' " +
			"INTO TABLE admission").isSuccess)
	}
	
	def createTableTest(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources/sharkfiles").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists test").isSuccess)
		assert(runSQLCmd(bigRClient, "CREATE TABLE test (key Int, val String)").isSuccess)
		assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test").isSuccess)

	}
	def createTableAirQuality(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists airquality").isSuccess)
		assert(runSQLCmd(bigRClient, "create table airquality ("
			+ "ozone int, solar_radiation int, wind double, temp int, month int, day int"
			+ ") row format delimited fields terminated by ','").isSuccess)
		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airquality.csv' " +
			"INTO TABLE airquality").isSuccess)
	}
	//	def createTableAirline(bigRClient: BigRClient) = {
	//		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
	//		assert(runSQLCmd(bigRClient, "drop table if exists airline").isSuccess)
	//		assert(runSQLCmd(bigRClient, "create table airline (v1 bigint, v2 double, v3 double, v4 double," +
	//			" v5 double, v6 double, v7 double, v8 double, v9 string, v10 double," +
	//			" v11 string, v12 double, v13 double, v14 double, v15 string, v16 string, v17 double)" +
	//			" row format delimited fields terminated by ','").isSuccess)
	//		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airline.csv' " +
	//			"INTO TABLE airline").isSuccess)
	//	}
	def createTableAirline(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists airline").isSuccess)
		assert(runSQLCmd(bigRClient, "create table airline (v1 int, v2 double, v3 double, v4 double," +
			" v5 double, v6 double, v7 double, v8 double, v9 string, v10 double," +
			" v11 string, v12 double, v13 double, v14 double, v15 double, v16 double, " +
			"v17 string, v18 string, v19 double, v20 double, v21 double, v22 double, v23" +
			" double, v24 double, v25 double, v26 double, v27 double, v28 double, v29 double)" +
			" row format delimited fields terminated by ','").isSuccess)
		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airline.csv' " +
			"INTO TABLE airline").isSuccess)
	}
	
//	def createTableAirlineTransform(bigRClient: BigRClient) = {
//		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
//		assert(runSQLCmd(bigRClient, "drop table if exists airline").isSuccess)
//		assert(runSQLCmd(bigRClient, "create table airline (v1 int, v2 double, v3 double, v4 double," +
//			" v5 double, v6 double, v7 double, v8 double, v9 string, v10 double," +
//			" v11 string, v12 double, v13 double, v14 double, v15 double, v16 double, " +
//			"v17 string, v18 string, v19 double, v20 double, v21 double, v22 double, v23" +
//			" double, v24 double, v25 double, v26 double, v27 double, v28 double, v29 double)" +
//			" row format delimited fields terminated by ','").isSuccess)
//		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airline.csv' " +
//			"INTO TABLE airline").isSuccess)
//	}
	
	def createTableAirlineWithNA(bigRClient: BigRClient) = {
		assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
		assert(runSQLCmd(bigRClient, "drop table if exists airline").isSuccess)
		assert(runSQLCmd(bigRClient, "create table airline (v1 int, v2 double, v3 double, v4 double," +
			" v5 double, v6 double, v7 double, v8 double, v9 string, v10 double," +
			" v11 string, v12 double, v13 double, v14 double, v15 double, v16 double, " +
			"v17 string, v18 string, v19 double, v20 double, v21 double, v22 double, v23" +
			" double, v24 double, v25 double, v26 double, v27 double, v28 double, v29 double)" +
			" row format delimited fields terminated by ','").isSuccess)
		assert(this.runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airlineWithNa.csv' " +
			"INTO TABLE airline").isSuccess)
	}
	
	def createTableRatings(bigRClient: BigRClient) {
	assert(runSQLCmd(bigRClient, "set shark.test.data.path=resources").isSuccess)
    assert(runSQLCmd(bigRClient, "drop table if exists ratings").isSuccess)
	assert(runSQLCmd(bigRClient, "create table ratings (userid int, movieid int, score double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess)
	assert(runSQLCmd(bigRClient, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/ratings.data' INTO TABLE ratings").isSuccess)  
  }
	
	def projectDDF(bigRClient: BigRClient, dcID: String, xCols: Array[Int], yCol: Int): String = {
	  val columnList = new ArrayList[String]
	  for (xCol <- xCols) {
	    columnList.add("{type: Column, index: " + xCol + "}")
	  }
	  columnList.add("{type: Column, index: " + yCol + "}")
	  val jsCreateVectors = String.format("{columns: [%s], dataContainerID: %s}", StringUtils.join(columnList, ", "), dcID);
	  val result= bigRClient.execute[SubsetResult]("Subset", jsCreateVectors)
	  
	  result.result.getDataContainerID
	}
} 
