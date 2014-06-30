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

import com.adatao.ML
import com.adatao.ML.LogisticRegressionModel
import com.adatao.pa.spark.types.ABigRClientTest
import org.junit.Assert._
import com.adatao.ML.LinearRegressionModel
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import com.adatao.ddf.ml.RocMetric
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import java.util.HashMap

/**
 *
 */
class MetricsSuite extends ABigRClientTest {

	private def loadTestFile: String = {
		//		this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
	}

	test("Test YtrueYpredict function") {

		createTableAdmission
		val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

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

		val trainer = new LogisticRegressionCRS(dataContainerId, Array(0,1), 2, columnsSummary, 1, 0.0, lambda, Array(37.285, -5.344, 1))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)

		val modelID = r.persistenceID

		//run prediction
		val predictor = new YtrueYpred(dataContainerId, modelID)
		val r2 = bigRClient.execute[YtrueYpredResult](predictor)
		val predictionResultId = r2.result.dataContainerID
		assert(r2.isSuccess)

	}
	
	/**
	 * this will test ROC execution
	 * success: if roc execution return success value
	 * fail: if not success
	 * for accuracy testing, please see MLMetricSuite
	 */
	test("Test ROC metric function") {

		createTableAdmission
		val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

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

		
		// fake the training with learningRate = 0.0
		val trainer = new LogisticRegressionCRS(dataContainerId, Array(0, 1), 2, columnsSummary, 1, 0.0, lambda, Array(37.285, -5.344, 1))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)
		println(">>>>>>model=" + r.result)
		val modelID = r.persistenceID
		
		println(">>>>>>>>>>>>>>>>>.modelID" + modelID)

		//run prediction
		val predictor = new YtrueYpred(dataContainerId, modelID)
		val r2 = bigRClient.execute[YtrueYpredResult](predictor)
		val predictionResultId = r2.result.dataContainerID
		assert(r2.isSuccess)
		
		println(">>>>>>>>>>>>>>>>>.predictionResultId=" + predictionResultId)

		//		//run ROC
		val alpha_length: Int = 10
		val executor = new ROC(predictionResultId, Array(0, 1), alpha_length)
		val ret = bigRClient.execute[RocMetric](executor)

		val metric = ret.result
		assert(ret.isSuccess)
		//this result is idential with confusion matrix unit test
//		assert(truncate(ret.result.pred(5)(1), 4) === 0.6220)
//		assert(truncate(ret.result.pred(5)(2), 4) === 0.3727)
//		assert(truncate(ret.result.auc, 4) === 0.6743)

	}

	test("R2 metric is correct") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

		// lm(mpg ~ wt, data=mtcars)
		// 37.285       -5.344
		// fake the training with learningRate = 0.0
		val trainer = new LinearRegression(dataContainerId, Array(0), 1, 1, 0.0, lambda, Array(37.285, -5.344))
		val r = bigRClient.execute[LinearRegressionModel](trainer)
		assert(r.isSuccess)

		val modelID = r.persistenceID
		

		val scorer = new R2Score(dataContainerId, modelID)
		val r2 = bigRClient.execute[Double](scorer)
		assert(r2.isSuccess)

		// summary(lm(mpg ~ wt, data=mtcars))
		// Multiple R-squared:  0.7528
		assertEquals(0.7528, r2.result, 0.0001)
	}

	test("Residuals metric is correct") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

		val trainer = new LinearRegression(dataContainerId, Array(0), 1, 1, 0.0, lambda, Array(37.285, -5.344))
		val r = bigRClient.execute[LinearRegressionModel](trainer)
		assert(r.isSuccess)

		val modelID = r.persistenceID

		val scorer = new Residuals(dataContainerId, modelID, Array(0), 1)
		val residuals = bigRClient.execute[Double](scorer)
		assert(residuals.isSuccess)

		println(">>>>>result =" + residuals.result)

	}

	test("smoke residuals metric") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select drat, vs from mtcars", true)
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

		val trainer = new LogisticRegressionCRS(dataContainerId, Array(0), 1, columnsSummary, 1, 0.0, lambda, Array(37.285, -5.344))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)

		val modelID = r.persistenceID

		val scorer = new Residuals(dataContainerId, modelID, Array(0), 1)
		val residuals = bigRClient.execute[ResidualsResult](scorer)
		assert(residuals.isSuccess)

		println(">>>>>residuals =" + residuals.result)

	}

	test("can get linear predictions") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)

		val dataContainerId = df.dataContainerID
		val lambda = 0.0
		// lm(mpg ~ wt, data=mtcars)
		// 37.285       -5.344
		// fake the training with learningRate = 0.0
		val trainer = new LinearRegression(dataContainerId, Array(0), 1, 1, 0.0, lambda, Array(37.285, -5.344))
		val r = bigRClient.execute[LinearRegressionModel](trainer)
		assert(r.isSuccess)

		val persistenceID = r.persistenceID

		val predictor = new YtrueYpred(dataContainerId, persistenceID)
		val r2 = bigRClient.execute[YtrueYpredResult](predictor)
		assert(r2.isSuccess)
		
//		val newid = r2.result.dataContainerID.replaceAll("SparkDDF-spark-","")
//		println(">>>>>>>> newid = " + newid + "\tr2.result.dataContainerID=" + r2.result.dataContainerID)
//
//		val fetcher = new FetchRows().setDataContainerID(newid).setLimit(32)
//		val r3 = bigRClient.execute[FetchRowsResult](fetcher)
//		assert(r3.isSuccess)
//
//		println(r3.result.data)
	}

	ignore("can get linear predictions categorical columns") {
		createTableAirline
			
		val df = this.runSQL2RDDCmd("select v4, v17, v18, v3 from airline", true)

		val dataContainerId = df.dataContainerID

		val lambda = 0.0
		val trainer = new LinearRegression(dataContainerId, Array(0, 1, 2), 3, 50, 0.01, lambda, null)
		val r = bigRClient.execute[LinearRegressionModel](trainer)

		assert(r.isSuccess)

		val persistenceID = r.persistenceID

		val predictor = new YtrueYpred(dataContainerId, persistenceID)
		val r2 = bigRClient.execute[YtrueYpredResult](predictor)
		assert(r2.isSuccess)

//		val fetcher = new FetchRows().setDataContainerID(r2.result.dataContainerID).setLimit(32)
//		val r3 = bigRClient.execute[FetchRowsResult](fetcher)
//		assert(r3.isSuccess)
//
//		println(r3.result.data)
	}
//	//
	test("can get logistic predictions") {

		createTableAdmission2
		val df = this.runSQL2RDDCmd("select * from admission", true)
		val dataContainerId = df.dataContainerID

		val lambda = 0.0

		// fake the training with learningRate = 0.0
		val trainer = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)
		val persistenceID = r.persistenceID

		val predictor = new YtrueYpred(dataContainerId, persistenceID)
		val r2 = bigRClient.execute[YtrueYpredResult](predictor)
		assert(r2.isSuccess)

//		val fetcher = new FetchRows().setDataContainerID(r2.result.dataContainerID).setLimit(32)
//		val r3 = bigRClient.execute[FetchRowsResult](fetcher)
//		assert(r3.isSuccess)
	}

	test("test confusion matrix") {

		createTableAdmission
		val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
		val dataContainerId = df.dataContainerID
		val lambda = 0.0

		// fake the training with learningRate = 0.0
		val trainer = new LogisticRegression(dataContainerId, Array(0, 1), 2, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)
		val persistenceID = r.persistenceID
		
		println(">>>>model r.result = " + r.result)

		val threshold = 0.5
		val executor = new BinaryConfusionMatrix(dataContainerId, persistenceID, threshold)
		val ret = bigRClient.execute[BinaryConfusionMatrixResult](executor)
		assert(ret.isSuccess)

		val cm = ret.result

		//		println(">>>>>>>>>>>>>>cm=" + cm.truePos + "\t" + cm.falsePos + "\t" + cm.trueNeg + "\t" + cm.falseNeg)

		// TODO: need to double check these results by hand
		assert(cm.truePos === 79)
		assert(cm.falsePos === 102)
		assert(cm.falseNeg === 48)
		assert(cm.trueNeg === 171)
		assert(400 === cm.truePos + cm.falsePos + cm.falseNeg + cm.trueNeg) // total count
	}
//
	test("smoke test for R2 metric - R2 metric works") {
		createTableMtcars
		val df = this.runSQL2RDDCmd("select drat, vs from mtcars", true)
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

		// lm(mpg ~ wt, data=mtcars)
		// 37.285       -5.344
		// fake the training with learningRate = 0.0
		val trainer = new LogisticRegressionCRS(dataContainerId, Array(0), 1, columnsSummary, 1, 0.0, lambda, Array(37.285, -5.344))
		val r = bigRClient.execute[LogisticRegressionModel](trainer)
		assert(r.isSuccess)

		val modelID = r.persistenceID
		assertTrue(modelID != null)

		val scorer = new R2Score(dataContainerId, modelID)
		val r2 = bigRClient.execute[Double](scorer)
		assert(r2.isSuccess)
		println(">>>>>result =" + r2.result)
		//		assertEquals(0.7528, r2.result, 0.0001)

		// summary(lm(mpg ~ wt, data=mtcars))
		// Multiple R-squared:  0.7528

	}
}