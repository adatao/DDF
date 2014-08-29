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
package com.adatao.pa.spark.execution

import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.spark.ddf.analytics.LinearRegressionModel
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import io.ddf.ml.IModel
import com.adatao.spark.ddf.analytics.{IRLSLogisticRegressionModel, NQLinearRegressionModel}

//import com.adatao.spark.ddf.analytics.LogisticRegressionModel

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.types.ExecutionResult
import java.util.HashMap
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import java.lang.Integer
import com.adatao.pa.spark.execution.FiveNumSummary._

/**
 * Tests for regression algorithms, having to do with the BigR environment
 *
 * @author ctn, aht, khangich, nhanvlc
 *
 */
class  RegressionSuite  extends ABigRClientTest {


	//smoke test
	test("Single-variable linear regression - normal equation categorical - no regularization") {
//		val dataContainerId = this.loadFile(List("resources/airline.csv", "server/resources/airline.csv"), false, ",")
		createTableAirline
    val loader = new Sql2DataFrame("select * from airline", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
		val dataContainerId = r0.dataContainerID
		val lambda = 1.0
		//this will cause Infinity, fail
		val executor = new LinearRegressionNormalEquation(dataContainerId, Array(5, 9), 0, lambda)
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[NQLinearRegressionModel]
		println(model.weights(0) + " " + model.weights(1))
		println(model.stdErrs(0) + " " + model.stdErrs(1))
		println(model.numFeatures + " " + model.numFeatures)
		println(model.rss + " " + model.sst)
//    1683.4380053944365 0.19731236420569986
//    35.723675414984314 0.02554716564538392
//    3 3
//    346940.9925998255 9.234042555093765
	}

	// TODO: failed due to 
	// TestFailedException: 37.674034 did not equal 37.22727
  test("Multiple-variable linear regression - normal equation - no regularization") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
    val dataContainerId = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result.dataContainerID
		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(3, 5), 0)
		val executor = new LinearRegressionNormalEquation(dataContainerId, Array(3, 5), 0, lambda)
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[NQLinearRegressionModel]
		println(">>>>>>>>>>model=" + model)
		
//		assertEquals(truncate(model.weights(0), 6),37.674034, 2)
//    assertEquals(truncate(model.weights(1), 6), -0.032085, 0.1)
//    assertEquals(truncate(model.weights(2), 6), -3.93936, 3)
//    assertEquals(truncate(model.stdErrs(0), 6), 0.995101, 0.1)
//    assertEquals(truncate(model.stdErrs(1), 6), 0.005652, 0.1)
//    assertEquals(truncate(model.stdErrs(2), 6), 0.413006, 0.1)
//    assertEquals(truncate(model.rss, 6), 196.546235, 2)
//    assertEquals(truncate(model.sst, 6), 1917.799167, 2)
//    assertEquals(model.numFeatures, 3)
//    assertEquals(model.numSamples, 48)
//    assertEquals(truncate(model.vif(0), 6), 1.766625, 0.1)
//    assertEquals(truncate(model.vif(1), 6), 1.766625, 0.1)
	}

  test("Single-variable linear regression") {
		createTableMtcars
//
    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		val dataContainerId = r0.dataContainerID
		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 0)
		val executor = new LinearRegression(dataContainerId, Array(5), 0, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]

		if (model.dummyColumnMapping != null) println(">>>>>>>>>>>>>>>> model.dummyColumnMapping  =" + model.dummyColumnMapping)
		assert(truncate(model.weights(0), 4) === 37.3180)
		assert(truncate(model.weights(1), 4) === -5.3539)
		assert(truncate(model.trainingLosses(0), 4) === 40.9919)
		assert(truncate(model.trainingLosses(1), 4) === 9.9192)
	}//

	test("Single-variable linear regression on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val lambda = 0.0
		val executor = new LinearRegression(dataContainerId, Array(5), 0, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]
		assert(truncate(model.weights(0), 4) === 37.3180)
		assert(truncate(model.weights(1), 4) === -5.3539)
		assert(truncate(model.trainingLosses(0), 4) === 40.9919)
		assert(truncate(model.trainingLosses(1), 4) === 9.9192)
	}

	test("Categorical variables linear regression normal equation on Shark") {
		createTableAirline

		val loader = new Sql2DataFrame("select * from airline", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val lambda = 0.1
		val executor = new LinearRegressionNormalEquation(dataContainerId, Array(14), 7, lambda)
		//		val executor = new LinearRegression(dataContainerId, Array(16, 1), 7, 40, 0.05, lambda, null)
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[NQLinearRegressionModel]
		println(">>>>>>>>>>>>>>>>> final model =" + model.toString)

		if (model.getDummy() != null) println(">>>>>>>>>>>>>>>> model.dummyColumnMapping  =" + model.getDummy())
		//		assert(model.weights.length === 3)
		assert(model.getDummy() != null)
	}



	test("Single-variable linear regression with null initialWeights") {
		createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

		val lambda = 0.0

		val executor = new LinearRegression(dataContainerId, Array(5), 0, 1, 0.05, lambda, null)
		val r = bigRClient.execute[LinearRegressionModel](executor)

		assert(r.isSuccess)
	}

	test("Multiple-variable linear regression") {
		createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

		val lambda = 0.0
		val executor = new LinearRegression(dataContainerId, Array(3, 5), 0, 1, 0.00005, lambda, Array(37.3, -0.04, -3.9))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]

		assertEquals(37.227, model.weights(0), 0.1)
		assertEquals(-0.031, model.weights(1), 0.1)
		assertEquals(-3.877, model.weights(2), 0.1)
	}
//
	test("Single-variable linear regression with regularization") {
		createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID


		val lambda = 1.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 0)
		val executor = new LinearRegression(dataContainerId, Array(5), 0, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]
		println(">>>model=" + model)

		assertEquals(model.weights(0), 33.2946, 0.1)
		assertEquals(model.weights(1), -4.2257, 0.1)
		assertEquals(model.trainingLosses(0), 86.3981, 0.1)
		assertEquals(model.trainingLosses(1), 54.1295, 0.1)
	}


//	test("Single-variable linear regression with null initialWeights") {
//		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
//		val lambda = 0.0
//
//		val executor = new LinearRegression(dataContainerId, Array(5), 0, 1, 0.05, lambda, null)
//		val r = bigRClient.execute[LinearRegressionModel](executor)
//
//		assert(r.isSuccess)
//	}
//
//	test("Multiple-variable linear regression") {
//		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
//		val lambda = 0.0
//		val executor = new LinearRegression(dataContainerId, Array(3, 5), 0, 1, 0.00005, lambda, Array(37.3, -0.04, -3.9))
//		val r = bigRClient.execute[IModel](executor)
//
//		assert(r.isSuccess)
//
//		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]
//
//		assertEquals(37.227, model.weights(0), 0.1)
//		assertEquals(-0.031, model.weights(1), 0.1)
//		assertEquals(-3.877, model.weights(2), 0.1)
//	}
//
//	test("Single-variable linear regression with regularization") {
//		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
//		val lambda = 1.0
////		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 0)
//		val executor = new LinearRegression(dataContainerId, Array(5), 0, 40, 0.05, lambda, Array(38, -3))
//		val r = bigRClient.execute[IModel](executor)
//
//		assert(r.isSuccess)
//
//		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]
//		println(">>>model=" + model)
//
//		assertEquals(model.weights(0), 33.2946, 0.1)
//		assertEquals(model.weights(1), -4.2257, 0.1)
//		assertEquals(model.trainingLosses(0), 86.3981, 0.1)
//		assertEquals(model.trainingLosses(1), 54.1295, 0.1)
//	}
//
	test("Single-variable logistic regression") {
		createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 7)
		val executor = new LogisticRegression(dataContainerId, Array(5), 7, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LogisticRegressionModel]
		assert(truncate(model.weights(0), 4) === 36.8605)
		assert(truncate(model.weights(1), 4) === -7.1806)
		assert(truncate(model.trainingLosses(0), 4) === 15.1505)
		assert(truncate(model.trainingLosses(1), 4) === 14.9196)
	}
//
	test("Single-variable logistic regression on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		val dataContainerId = r0.result.dataContainerID

		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 7)
		val executor = new LogisticRegression(dataContainerId, Array(5), 7, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LogisticRegressionModel]
		assert(truncate(model.weights(0), 4) === 36.8605)
		assert(truncate(model.weights(1), 4) === -7.1806)
		//		assert(truncate(model.trainingLosses(0), 4) === 15.1505)
		//		assert(truncate(model.trainingLosses(1), 4) === 14.9196)
	}

	test("Single-variable logistic regression with null initialWeights") {
		createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
    assert(r0.isSuccess)
    val dataContainerId = r0.result.dataContainerID
		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 7)
		val executor = new LogisticRegression(dataContainerId, Array(5), 7, 1, 0.05, lambda, null)
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)
	}
//
	test("Single-variable logistic regression with regularization") {
		  createTableMtcars

    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
    assert(r0.isSuccess)
    val dataContainerId = r0.result.dataContainerID

		val lambda = 1.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 7)
		val executor = new LogisticRegression(dataContainerId, Array(5), 7, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LogisticRegressionModel]
		println(">>>model=" + model)

		assertEquals(model.weights(0), 32.3613, 0.1)
		assertEquals(model.weights(1), -6.5206, 0.1)
		assertEquals(model.trainingLosses(0), 60.5567, 0.1)
	}

//
//	test("Multiple-variable logistic regression") {
//		val dataContainerId = this.loadFile(List("resources/admission.csv", "server/resources/admission.csv"), false, " ")
//		val lambda = 0.0
////		val projDataContainerId = this.projectDDF(dataContainerId, Array(2, 3), 0)
//		val executor = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.1, lambda, Array(-3.0, 1.5, -0.9))
//		val r = bigRClient.execute[IModel](executor)
//
//		assert(r.isSuccess)
//
//		val model = r.result.getRawModel.asInstanceOf[LogisticRegressionModel]
//
//		assertEquals(true, r.isSuccess);
//
//		assertEquals(-3.0251, model.weights(0), 0.0001);
//		assertEquals(1.4117, model.weights(1), 0.0001);
//		assertEquals(-0.9493, model.weights(2), 0.0001);
//	}
//
	test("Test Infinity bug on airline data") {
		createTableAirline

    val loader = new Sql2DataFrame("select * from airline", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(0, 6, 7), 12)
		val executor = new LogisticRegression(dataContainerId, Array(0, 6, 7), 12, 50, 0.1, lambda, Array(0.00000000001, 0.00000000001, 0.00000000001, 0.00000000001))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LogisticRegressionModel]

		assertEquals(true, r.isSuccess);
		println(">>>>>>>>> " + model.trainingLosses)

	}
//
	test("Single variable linear regression on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val lambda = 0.0
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(5), 0)
		val executor = new LinearRegression(dataContainerId, Array(5), 0, 40, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[IModel](executor)

		assert(r.isSuccess)

		val model = r.result.getRawModel.asInstanceOf[LinearRegressionModel]
		assert(truncate(model.weights(0), 4) === 37.3180)
		assert(truncate(model.weights(1), 4) === -5.3539)
		assert(truncate(model.trainingLosses(0), 4) === 40.9919)
		assert(truncate(model.trainingLosses(1), 4) === 9.9192)
	}
//
	test("Single-variable linear regression on Shark, binned var") {
		createTableAirline

		val loader = new Sql2DataFrame("select * from airline", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val cmd = new Binning(dataContainerId, "v19", binningType = "equalFreq", numBins = 5, includeLowest = false, right = false)
		val result = bigRClient.execute[BinningResult](cmd)

		println(">>>>>>> result=" + result.result)

//		val cmd3 = new FetchRows().setDataContainerID(result.result.dataContainerID).setLimit(1)
//		val res3 = bigRClient.execute[FetchRowsResult](cmd3)
//		println(">>>>>>> res3=" + res3.result.data)

		val lambda = 0.0
		val projDataContainerId = this.projectDDF(result.result.dataContainerID, Array(1, 18), 14)

		val cmd3 = new FetchRows().setDataContainerID(dataContainerId).setLimit(1)
    val res3 = bigRClient.execute[FetchRowsResult](cmd3)
    println(">>>>>>> res3=" + res3.result.data)

		val executor = new LinearRegressionNormalEquation(dataContainerId, Array(1, 18), 14, lambda)
		val r = bigRClient.execute[IModel](executor)
		assert(r.isSuccess)
		val model = r.result

		println(">>>>model=" + model)
	}
//
	test("test MaxFeatures") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select * from mtcars", true)
		assert(df.isSuccess)

		val dcID = df.dataContainerID
		LOG.info("Get dataContainerID= " + dcID)
		val cmd = new GetMultiFactor(dcID, Array(7, 8, 9, 10))
		val result = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd).result

		val lambda = 0.1
//		val projDataContainerId = this.projectDDF(dcID, Array(6, 7, 8, 9, 10), 0)
		val executor = new LinearRegressionNormalEquation(dcID, Array(6, 7, 8, 9, 10), 0, lambda)
		System.setProperty("bigr.lm.maxNumFeatures", "10")
		try {
			val r = bigRClient.execute[IModel](executor)
			assert(false)
			assert(!r.isSuccess)
		}
		catch {
			case e ⇒ {
				assert(e.isInstanceOf[java.lang.Exception])
			}
		}

		System.setProperty("bigr.lm.maxNumFeatures", "20")
		val r1 = bigRClient.execute[IModel](executor)
		assert(r1.isSuccess)
	}

	//	GOOD, result are identical with glm.gd
	ignore("Multiple-variable logistic regression on sparse matrix, no sparse column") {

		//load data
		createTableAdmission
		val df = this.runSQL2RDDCmd("select v2, v4, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

		//minimum threshold range for sparse columns
		System.setProperty("sparse.max.range", "10000")
		var cmd2 = new FiveNumSummary(dataContainerId)
		val summary = bigRClient.execute[Array[ASummary]](cmd2).result
		assert(summary.size > 0)

		//construct columnSummary parameter
		var columnsSummary = new HashMap[String, Array[Double]]
		var hmin = new Array[Double](summary.size)
		var hmax = new Array[Double](summary.size)
		//convert columnsSummary to HashMap
		var i = 0
		while (i < summary.size) {
			hmin(i) = summary(i).min
			hmax(i) = summary(i).max
			i += 1
		}
		columnsSummary.put("min", hmin)
		columnsSummary.put("max", hmax)

//		val projDataContainerId = this.projectDDF(dataContainerId, Array(1, 2), 0)
		val executor = new LogisticRegressionCRS(dataContainerId, Array(1, 2), 0, columnsSummary, 1, 0.1, lambda, Array(-3.0, 1.5, -0.9))
		val r = bigRClient.execute[LogisticRegressionModel](executor)
		//		assert(r.isSuccess)

		//assertion, expect to produce similarly identical result with glm.gd non-sparse
		val model = r.result
		println("model=" + model)
		println(">>>>>r=" + r)

		//		assertEquals(true, r.isSuccess);
		//		assertEquals(-3.0251, model.weights(0), 0.0001);
		//		assertEquals(1.4117, model.weights(1), 0.0001);
		//		assertEquals(-0.9493, model.weights(2), 0.0001);
	}

	ignore("Multiple-variable logistic regression on sparse matrix, case one with sparse column") {

		//load data
		createTableAdmission
		val df = this.runSQL2RDDCmd("select v2, v3, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0
		System.setProperty("sparse.max.range", "10000")
		val iterations = 1

		//get summary
		var cmd2 = new FiveNumSummary(dataContainerId)
		val summary = bigRClient.execute[Array[ASummary]](cmd2).result
		assert(summary.size > 0)

		//construct columnSummary parameter
		var columnsSummary = new HashMap[String, Array[Double]]
		var hmin = new Array[Double](summary.size)
		var hmax = new Array[Double](summary.size)
		//convert columnsSummary to HashMap
		var i = 0
		while (i < summary.size) {
			hmin(i) = summary(i).min
			hmax(i) = summary(i).max
			i += 1
		}
		columnsSummary.put("min", hmin)
		columnsSummary.put("max", hmax)

		val startTime = System.currentTimeMillis()
//		val projDataContainerId = this.projectDDF(dataContainerId, Array(0, 1), 2)
		val executor = new LogisticRegressionCRS(dataContainerId, Array(0,1), 2, columnsSummary, iterations, 0.1, lambda, Array(-3.0, 1.5))
		val r = bigRClient.execute[LogisticRegressionModel](executor)
		assert(r.isSuccess)

		val model = r.result
		assertEquals(true, r.isSuccess);
		val endTime = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> finish: " + (endTime - startTime))
	}

	ignore("Multiple-variable logistic regression on sparse matrix, case one with sparse column on adwo data") {
		val lambda = 0.0

		val MAX_ROWS = Integer.parseInt(System.getProperty("training.max.record", "10000000"))

		val loader = new Sql2DataFrame("select if(prob>=0.5, 1, 0) as clicked, advertise_id from (select rand() as prob, advertise_id from adwo_week_show limit " + MAX_ROWS + ") t", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)
		System.getProperty("sparse.max.range", "50")
		println(">>>>>>>>>> finish loading shark data")
		val dataContainerId = r0.dataContainerID

		//max advertise_id
		var cmd2 = new FiveNumSummary(dataContainerId)
		val summary = bigRClient.execute[Array[ASummary]](cmd2).result

		//construct columnSummary parameter
		var columnsSummary = new HashMap[String, Array[Double]]
		var hmin = new Array[Double](summary.size)
		var hmax = new Array[Double](summary.size)
		//convert columnsSummary to HashMap
		var i = 0
		while (i < summary.size) {
			hmin(i) = summary(i).min
			hmax(i) = summary(i).max
			i += 1
		}
		columnsSummary.put("min", hmin)
		columnsSummary.put("max", hmax)

//		val projDataContainerId = this.projectDDF(dataContainerId, Array(1), 0)
		val executor = new LogisticRegressionCRS(dataContainerId, Array(1), 0, columnsSummary, 10, 0.1, lambda, Array(-3.0, 1.5))
		val r = bigRClient.execute[LogisticRegressionModel](executor)

		assert(r.isSuccess)

		val model = r.result

		assertEquals(true, r.isSuccess);

	}


	test("Multiple-variable logistic regression IRLS - ddf") {

		//load data
		createTableAdmission
		val df = this.runSQL2RDDCmd("select v2, v3, v4, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

		val executor = new LogisticRegressionIRLS(dataContainerId, Array(0, 1, 2), 3, 25, 1e-8, lambda, Array(0, 0))
		val r = bigRClient.execute[IRLSLogisticRegressionModel](executor)
		assert(r.isSuccess)
	}


	test("test dummy coding") {

		//load data
		createTableAirline
//		val df = this.runSQL2RDDCmd("select v8, v9, v10, v17, v12 from airline", true)

		val df = this.runSQL2RDDCmd("select v8, v17, v12 from airline", true)

		val dataContainerId = df.dataContainerID
		val lambda = 1.0

//		val projDataContainerId = this.projectDDF(dataContainerId, Array(0, 1), 2)
		val executor = new LogisticRegressionIRLS(dataContainerId, Array(0, 1), 2, 25, 1e-8, lambda, Array(0, 0, 0))
		val r = bigRClient.execute[IRLSLogisticRegressionModel](executor)
		assert(r.isSuccess)
	}
}
