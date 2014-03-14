package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import scala.collection.JavaConversions._
import com.adatao.pa.spark.execution.LoadHiveTable.LoadHiveTableResult
import com.adatao.pa.spark.execution.NRow.NRowResult

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 22/10/13
 * Time: 12:44 PM
 * To change this template use File | Settings | File Templates.
 */
class SqlExecutionSuite extends ABigRClientTest {

	test("Test Sql2ListString"){
		this.runSQLCmd("drop table if exists test")
		this.runSQLCmd("CREATE TABLE test (key Int, val String)")
		this.runSQLCmd("LOAD DATA LOCAL INPATH 'resources/sharkfiles/kv1.txt' INTO TABLE test")

		val sqlRes= this.runSQLCmd("SELECT * FROM test LIMIT 50")
		assert(sqlRes.isSuccess)

		val sqlRes2= this.runSQLCmd("SELECT count(*) FROM test")
		assert(sqlRes2.isSuccess)
	}

	test("Test Sql2DataFrame"){
		this.runSQLCmd("drop table if exists test")
		this.runSQLCmd("CREATE TABLE test (uid Int, type String)")
		this.runSQLCmd("LOAD DATA LOCAL INPATH 'resources/sharkfiles/kv3.txt' INTO TABLE test")

		val df= this.runSQL2RDDCmd("SELECT * FROM test", true)
		assert(df.isSuccess)
		val dataContainerID= df.dataContainerID
		LOG.info("got dataContainerID = " + dataContainerID)
		assert(df.metaInfo.length == 2)
		assert(df.metaInfo(0).getHeader == "uid")
		assert(df.metaInfo(1).getHeader == "type")

		val params= String.format("{dataContainerID: %s, limit: 100}", dataContainerID)
		val result= bigRClient.execute[FetchRowsResult]("FetchRows", params)

		assert(result.isSuccess)
		val listData=result.result.getData
		for(data <- listData){
			LOG.info("FetchVectors result: " + data.mkString(", "))
		}
	}

	test("Test LoadHiveTable"){
		this.runSQLCmd("drop table if exists test")
		this.runSQLCmd("CREATE TABLE test (uid Int, type String)")
		this.runSQLCmd("LOAD DATA LOCAL INPATH 'resources/sharkfiles/kv3.txt' INTO TABLE test")

		val cmd= new LoadHiveTable().setTableName("test")

		val result= bigRClient.execute[LoadHiveTableResult](cmd)
		assert(result.isSuccess)
		val dcId= result.result.getDataContainerID
		LOG.info("Got dataContainerID= " + dcId)
		assert(result.result.metaInfo(0).getHeader == "uid")
		assert(result.result.metaInfo(1).getHeader == "type")

		val params= String.format("{dataContainerID: %s, limit: 100}", dcId)
		val result2 = bigRClient.execute[FetchRowsResult]("FetchRows", params)
		assert(result2.isSuccess)

		val data= result2.result.getData
		for(x <- data){
			LOG.info("FetchVectors result: " + x.mkString(", "))
		}
	}

	test("Test NRow sql"){
		this.runSQLCmd("drop table if exists test")
		this.runSQLCmd("CREATE TABLE test (uid INT, type STRING)")
		this.runSQLCmd("LOAD DATA LOCAL INPATH 'resources/sharkfiles/kv3.txt' INTO TABLE test")
		val df= this.runSQL2RDDCmd("SELECT * FROM test", true)
		assert(df.isSuccess)

		val dcID= df.dataContainerID
		val params= String.format("{dataContainerID: %s}", dcID)

		val result= bigRClient.execute[NRowResult]("NRow", params)

		assert(result.isSuccess)
		assert(result.result.nrow == 25)
	}
}

