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

import com.adatao.spark.ddf.analytics
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import com.adatao.pa.spark.types.ABigRClientTest
import org.junit.Assert._
import com.adatao.spark.ddf.analytics.LinearRegressionModel
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import io.ddf.ml.{IModel, RocMetric}
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import java.util.HashMap
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.NRow.NRowResult

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


    val trainer = new LogisticRegression(dataContainerId, Array(0, 1), 2, 1, 0.0, lambda, Array(37.285, -5.344, 1))
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)

    val modelID = r.result.getName

    //run prediction
    val predictor = new YtrueYpred(dataContainerId, modelID)
    val r2 = bigRClient.execute[DataFrameResult](predictor)
    val predictionResultId = r2.result.dataContainerID
    assert(r2.isSuccess)

  }
  //
  //	/**
  //	 * this will test ROC execution
  //	 * success: if roc execution return success value
  //	 * fail: if not success
  //	 * for accuracy testing, please see MLMetricSuite
  //	 */
  test("Test ROC metric function") {

    createTableAdmission
    val df = this.runSQL2RDDCmd("select * from admission", true)
    val dataContainerId = df.dataContainerID
    val lambda = 0.0

    // fake the training with learningRate = 0.0
//    val trainer = new LogisticRegression(dataContainerId, Array(0, 1), 2, 1, 0.0, lambda, Array(37.285, -5.344, 1))
    val trainer = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.1, lambda, Array(-3.0, 1.5, -0.9))
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)
    println(">>>>>>model=" + r.result)
    val modelID = r.result.getName

    println(">>>>>>>>>>>>>>>>>.modelID" + modelID)

    //run prediction
    val predictor = new YtrueYpred(dataContainerId, modelID)
    val r2 = bigRClient.execute[DataFrameResult](predictor)
    val predictionResultId = r2.result.dataContainerID
    assert(r2.isSuccess)

    println(">>>>>>>>>>>>>>>>>.predictionResultId=" + predictionResultId)

    val alpha_length: Int = 10
    val executor = new ROC(predictionResultId, alpha_length)
    val ret = bigRClient.execute[RocMetric](executor)

    val metric = ret.result
    
    println(">>>>>>>>>> metric ")
    assert(ret.isSuccess)
    //this result is idential with confusion matrix unit test
    assert(truncate(metric.auc, 4) === 0.6691)
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
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)

    val modelID = r.result.getName


    val scorer = new R2Score(dataContainerId, modelID)
    val r2 = bigRClient.execute[Double](scorer)
    assert(r2.isSuccess)

    // summary(lm(mpg ~ wt, data=mtcars))
    // Multiple R-squared:  0.7528
    assertEquals(0.7528, r2.result, 0.0001)
  }
  //
  test("Residuals metric is correct") {
    createTableMtcars
    val df = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
    val dataContainerId = df.dataContainerID
    val lambda = 0.0

    val trainer = new LinearRegression(dataContainerId, Array(0), 1, 1, 0.0, lambda, Array(37.285, -5.344))
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)

    val modelID = r.result.getName

    val scorer = new Residuals(dataContainerId, modelID, Array(0), 1)
    val residuals = bigRClient.execute[DataFrameResult](scorer)
    assert(residuals.isSuccess)

    val nrow = new NRow
    nrow.setDataContainerID(residuals.result.dataContainerID)
    val numRow = bigRClient.execute[NRowResult](nrow)
    assert(numRow.result.nrow === 32)

  }

  test("smoke test residuals metric") {
    createTableMtcars
//    val df = this.runSQL2RDDCmd("select * from mtcars", true)
    val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
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

    val trainer = new LogisticRegression(dataContainerId, Array(0, 1), 2, 1, 0.0, lambda, Array(37.285, -5.344, 1))
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)

    val modelID = r.result.getName

    val scorer = new Residuals(dataContainerId, modelID, Array(0), 1)
    val residuals = bigRClient.execute[DataFrameResult](scorer)
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
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)

    val persistenceID = r.result.getName

    val predictor = new YtrueYpred(dataContainerId, persistenceID)
    val r2 = bigRClient.execute[DataFrameResult](predictor)
    assert(r2.isSuccess)

  }

  test("can get linear predictions categorical columns") {
    createTableAirline

    val df = this.runSQL2RDDCmd("select v4, v17, v18, v3 from airline", true)

    val dataContainerId = df.dataContainerID

    val lambda = 0.0
    val trainer = new LinearRegression(dataContainerId, Array(0, 1, 2), 3, 50, 0.01, lambda, null)
    val r = bigRClient.execute[IModel](trainer)

    assert(r.isSuccess)

    val persistenceID = r.result.getName

    val predictor = new YtrueYpred(dataContainerId, persistenceID)
    val r2 = bigRClient.execute[DataFrameResult](predictor)
    assert(r2.isSuccess)

    //		val fetcher = new FetchRows().setDataContainerID(r2.result.dataContainerID).setLimit(32)
    //		val r3 = bigRClient.execute[FetchRowsResult](fetcher)
    //		assert(r3.isSuccess)
    //
    //		println(r3.result.data)
  }
  ////	//
  test("can get logistic predictions") {

    createTableAdmission2
    val df = this.runSQL2RDDCmd("select * from admission", true)
    val dataContainerId = df.dataContainerID

    val lambda = 0.0

    // fake the training with learningRate = 0.0
    val trainer = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)
    val persistenceID = r.result.getName

    val predictor = new YtrueYpred(dataContainerId, persistenceID)
    val r2 = bigRClient.execute[DataFrameResult](predictor)
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
    val r = bigRClient.execute[IModel](trainer)
    assert(r.isSuccess)
    val persistenceID = r.result.getName

    println(">>>>model r.result = " + r.result)

    val threshold = 0.5
    val executor = new BinaryConfusionMatrix(dataContainerId, persistenceID, threshold)
    val ret = bigRClient.execute[BinaryConfusionMatrixResult](executor)
    assert(ret.isSuccess)

    val cm = ret.result

    // TODO: need to double check these results by hand
    assert(cm.truePos === 79)
    assert(cm.falsePos === 102)
    assert(cm.falseNeg === 48)
    assert(cm.trueNeg === 171)
    assert(400 === cm.truePos + cm.falsePos + cm.falseNeg + cm.trueNeg) // total count
  }
}
