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

package com.adatao.pa.spark

import com.adatao.pa.spark.execution.LoadHiveTable
import com.adatao.pa.spark.types.ABigRClientTest
import shark.api.JavaSharkContext
import shark.SharkEnv
import com.adatao.pa.spark.types.ATestBase
import com.adatao.pa.spark.DataManager.SharkDataFrame
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.ddf.DDFManager
import com.adatao.spark.ddf.SparkDDFManager

/**
 * @author bachbui
 */
class SharkDataFrameSuite extends ATestBase { //ABigRClientTest {
	private val SPARK_MASTER = System.getenv("SPARK_MASTER");
	private val SPARK_HOME = System.getenv("SPARK_HOME");
	private val RSERVER_JAR = System.getenv("RSERVER_JAR");
	
	test("Test loadTable and loadTableFromQuery on DataManager.SharkDataFrame"){
		// val jobJars = RSERVER_JAR.split(",");
        val ddfManager = DDFManager.get("spark").asInstanceOf[SparkDDFManager]
		// val sharkContext = SharkEnv.initWithJavaSharkContext(new JavaSharkContext(SPARK_MASTER, "BigR", SPARK_HOME, jobJars));
        val sharkContext = ddfManager.getJavaSharkContext()
//		System.setProperty("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
//		System.setProperty("spark.kryo.registrator", classOf[shark.KryoRegistrator].getName)
		sharkContext.sql("drop table if exists test");
		sharkContext.sql("CREATE EXTERNAL TABLE test (year	INT, month	string, dayofmonth	INT, dayofweek	INT, deptime	INT, crsdeptime	INT," +
						" arrtime	INT, crsarrtime	INT, uniquecarrier	string	, flightnum	INT, tailnum	string	, actualelapsedtime	INT, " +
						"crselapsedtime	INT, airtime	INT, arrdelay	INT, depdelay	INT, origin	string	, dest	string	, distance	INT, " +
						"taxiin	INT, taxiout	INT, cancelled	INT, cancellationcode	string	, diverted	string	, carrierdelay	INT, " +
						"weatherdelay	INT, nasdelay	INT, securitydelay	INT, lateaircraftdelay	INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' " +
						"STORED AS TEXTFILE LOCATION '/works/adatao/BigR/server/resources/sharkfiles/dataframeloading/'")
		var df = new DataManager.SharkDataFrame()
		df.loadTable(sharkContext, "test", true);
		var metaData = df.metaInfo
		assert(metaData(0).getHeader() === "year")
		assert(metaData(0).getType() === "int")
		assert(metaData(9).getHeader() === "flightnum")
		assert(metaData(9).getType() === "int")
		assert(metaData(22).getHeader() === "cancellationcode")
		assert(metaData(22).getType() === "string")
		assert(metaData(28).getHeader() === "lateaircraftdelay")
		assert(metaData(28).getType() === "int")		
		
		df.getDataTable(Array(5,0),4).collect().foreach(r => println("Result >>>>>" +r))
		
		sharkContext.sql("drop table if exists "+df.tableName);
		
		
		
//		df = new DataManager.SharkDataFrame()
//		df.loadTableFromQuery(sharkContext, "select year,flightnum,cancellationcode,lateaircraftdelay from test", true);
//		metaData = df.metaInfo
//		assert(metaData(0).getHeader() === "year")
//		assert(metaData(0).getType() === "int")
//		assert(metaData(1).getHeader() === "flightnum")
//		assert(metaData(1).getType() === "int")
//		assert(metaData(2).getHeader() === "cancellationcode")
//		assert(metaData(2).getType() === "string")
//		assert(metaData(3).getHeader() === "lateaircraftdelay")
//		assert(metaData(3).getType() === "int")		
//		sharkContext.sql("drop table if exists "+df.tableName);
		
		sharkContext.sql("drop table if exists test");
		sharkContext.stop();
 	}
}

//MetaInfo [header=year, type=int, columnNo=-1]
//MetaInfo [header=month, type=int, columnNo=-1]
//MetaInfo [header=dayofmonth, type=int, columnNo=-1]
//MetaInfo [header=dayofweek, type=int, columnNo=-1]
//MetaInfo [header=deptime, type=int, columnNo=-1]
//MetaInfo [header=crsdeptime, type=int, columnNo=-1]
//MetaInfo [header=arrtime, type=int, columnNo=-1]
//MetaInfo [header=crsarrtime, type=int, columnNo=-1]
//MetaInfo [header=uniquecarrier, type=string, columnNo=-1]
//MetaInfo [header=flightnum, type=int, columnNo=-1]
//MetaInfo [header=tailnum, type=string, columnNo=-1]
//MetaInfo [header=actualelapsedtime, type=int, columnNo=-1]
//MetaInfo [header=crselapsedtime, type=int, columnNo=-1]
//MetaInfo [header=airtime, type=int, columnNo=-1]
//MetaInfo [header=arrdelay, type=int, columnNo=-1]
//MetaInfo [header=depdelay, type=int, columnNo=-1]
//MetaInfo [header=origin, type=string, columnNo=-1]
//MetaInfo [header=dest, type=string, columnNo=-1]
//MetaInfo [header=distance, type=int, columnNo=-1]
//MetaInfo [header=taxiin, type=int, columnNo=-1]
//MetaInfo [header=taxiout, type=int, columnNo=-1]
//MetaInfo [header=cancelled, type=int, columnNo=-1]
//MetaInfo [header=cancellationcode, type=string, columnNo=-1]
//MetaInfo [header=diverted, type=string, columnNo=-1]
//MetaInfo [header=carrierdelay, type=int, columnNo=-1]
//MetaInfo [header=weatherdelay, type=int, columnNo=-1]
//MetaInfo [header=nasdelay, type=int, columnNo=-1]
//MetaInfo [header=securitydelay, type=int, columnNo=-1]
//MetaInfo [header=lateaircraftdelay, type=int, columnNo=-1]
