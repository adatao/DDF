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

package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.{ ABigRClientTest }
import com.adatao.pa.thrift.generated.{ JsonResult, JsonCommand }
import com.google.gson.Gson
import com.adatao.pa.spark.execution.Sql2ListString.Sql2ListStringResult
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.pa.spark.execution.FiveNumSummary.{ASummary}
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.execution.AdvanceSummary.AdvanceSummaryResult

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 24/7/13
 * Time: 5:11 PM
 * To change this template use File | Settings | File Templates.
 */
class FiveNumSuite extends ABigRClientTest {

	test("test FiveNumSummary") {
		this.runSQLCmd("set shark.test.data.path=resources")
		this.runSQLCmd("drop table if exists mtcars")
		this.runSQLCmd("CREATE TABLE mtcars (mpg double, cyl string, disp string, hp int, " +
			"                   drat double, wt double, qsec string, vs string, am int, gear int, carb" +
			" int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
		this.runSQLCmd("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/mtcars' " +
			"INTO TABLE mtcars")
		val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
		assert(df.isSuccess)
		val dcID = df.dataContainerID
		LOG.info("Got dataConainterID = " + dcID)

		// var cmd1 = new GetFactor().setDataContainerID(dcID).setColumnName("carb")
		// bigRClient.execute[GetFactor.GetFactorResult](cmd1)

		var cmd2 = new FiveNumSummary(dcID)
		val res = bigRClient.execute[Array[ASummary]](cmd2).result
		LOG.info("result for string col = " + res(6).min +", " + res(6).max)
		assert(res(0).first_quartile === 15.2)
		assert(res(0).third_quartile === 22.15)
		assert(res(0).median === 19.20)
		assert(res(0).min === 10.40)
		assert(res(0).max === 33.90)


		expectResult(res(4).first_quartile)(3.080)
		expectResult(res(4).third_quartile)(3.913333333333333)
		expectResult(res(4).median)(3.69)

		assert(res(6).min.equals(Double.NaN))
		assert(res(6).median.equals(Double.NaN))
		assert(res(6).third_quartile.equals(Double.NaN))

		expectResult(res(10).first_quartile)(2.0)
		expectResult(res(10).third_quartile )( 4.0)
		expectResult(res(10).median )( 2.0)
		expectResult(res(10).min )( 1.0)
		expectResult(res(10).max )( 8.0)
	}
	
	test("test AdvanceSummary") {
    this.runSQLCmd("set shark.test.data.path=resources")
    this.runSQLCmd("drop table if exists airline")
    this.runSQLCmd("create table airline (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int, CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    this.runSQLCmd("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/airline.csv' " +
      "INTO TABLE mtcars")
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataConainterID = " + dcID)

    // var cmd1 = new GetFactor().setDataContainerID(dcID).setColumnName("carb")
    // bigRClient.execute[GetFactor.GetFactorResult](cmd1)

    var cmd2 = new AdvanceSummary(dcID)
    val res = bigRClient.execute[AdvanceSummaryResult](cmd2).result
    
    println(">>>> advanceSummary return:" + res.toString())
    
  }
}
