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
import com.adatao.ML.RocObject
import io.ddf.ml.{RocMetric, IModel}

/**
 *
 */
class CrossValidationSuite extends ABigRClientTest {

  private def loadTestFile: String = {
    this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
    //		this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
  }

//  test("CVRandomSplit") {
//    createTableMtcars
//    val df = this.runSQL2RDDCmd("select * from mtcars", true)
//
//    val dataContainerId = df.dataContainerID //this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, " ")
//    val splitter = new CVRandomSplit(dataContainerId, 5, 0.75, 42)
//    val r = bigRClient.execute[Array[Array[String]]](splitter)
//    assert(r.isSuccess)
//    println(r.result)
//    assert(r.result.length === 5)
//    r.result.foreach {
//      split ⇒
//        assert(split.length === 2)
//    }
//  }
//
//  test("CVKFoldSplit") {
//    //		val dataContainerId = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, " ")
//    createTableMtcars
//    val df = this.runSQL2RDDCmd("select * from mtcars", true)
//
//    val dataContainerId = df.dataContainerID
//
//    val splitter = new CVKFoldSplit(dataContainerId, 5, 42)
//    val r = bigRClient.execute[Array[Array[String]]](splitter)
//    assert(r.isSuccess)
//    println(r.result)
//    assert(r.result.length === 5)
//    r.result.foreach {
//      split ⇒
//        assert(split.length === 2)
//    }
//  }
//
//  test("R2 score on CVRandomSplit") {
//    createTableMtcars
//    val df = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
//    val dataContainerId = df.dataContainerID
//
//    val splitter = new CVRandomSplit(dataContainerId, 1, 0.75, 42)
//    val r = bigRClient.execute[Array[Array[String]]](splitter)
//    assert(r.isSuccess)
//    println(r.result)
//    assert(r.result.length === 1)
//    r.result.foreach {
//      split ⇒
//        val Array(train, test) = split
//
//        val trainer = new LinearRegression(train, Array(0), 1, 1, 0.0, 0.0, Array(37.285, -5.344))
//        val r = bigRClient.execute[IModel](trainer)
//        assert(r.isSuccess)
//
//        val persistenceID = r.result.getName
//        println(">>>>> persistenceID = " + persistenceID)
//        val scorer = new R2Score(test, persistenceID)
//        val r2 = bigRClient.execute[Double](scorer)
//        assert(r2.isSuccess)
//
//        println("!!! AHT !!! " + r2.result)
//      // no assertion here because result are dependent on random draw
//      // though deterministic given the seed
//    }
//  }
//
//  //	test("R2 score on CVRandomSplit on Shark") {
//  //		createTableMtcars
//  //
//  //		val r0 = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
//  ////		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
//  //		assert(r0.isSuccess)
//  //
//  //		val dataContainerId = r0.dataContainerID
//  //
//  //		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
//  //		val r = bigRClient.execute[Array[Array[String]]](splitter)
//  //		assert(r.isSuccess)
//  //		println(r.result)
//  //		assert(r.result.length === 1)
//  //		r.result.foreach { split ⇒
//  //			val Array(train, test) = split
//  //
//  //			// fake the training with learningRate = 0.0
//  //			val trainer = new LinearRegression(train, Array(0), 1, 1, 0.0, 0.0, Array(37.285, -5.344))
//  //			val r = bigRClient.execute[LinearRegressionModel](trainer)
//  //			assert(r.isSuccess)
//  //
//  //			val persistenceID = r.persistenceID
//  //
//  //			val scorer = new R2Score(test, Array(0), 1, persistenceID)
//  //			val r2 = bigRClient.execute[Double](scorer)
//  //			assert(r2.isSuccess)
//  //
//  //			println("!!! AHT !!! " + r2.result)
//  //			// no assertion here because result are dependent on random draw
//  //			// though deterministic given the seed
//  //		}
//  //	}
//  //
//  	test("LinearRegression on CVRandomSplit on Shark") {
//  		createTableMtcars
//
//  		val r0 = this.runSQL2RDDCmd("select wt, mpg from mtcars", true)
//  		assert(r0.isSuccess)
//
//  		val dataContainerId = r0.dataContainerID
//
//  		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
//  		val r = bigRClient.execute[Array[Array[String]]](splitter)
//  		assert(r.isSuccess)
//  		println(r.result)
//  		assert(r.result.length === 1)
//  		r.result.foreach { split ⇒
//  			val Array(train, test) = split
//
//  			// fake the training with learningRate = 0.0
//  			//val trainer = new LinearRegressionNormalEquation(train, Array(5), 0, 0.0)
//  			val trainer = new Kmeans(train, Array(0,1), 10, 10, initializationMode =  "random")
//        val r = bigRClient.execute[IModel](trainer)
//  			assert(r.isSuccess)
//  		}
//  	}

  test("Test ROC metric with CV random split") {
    createTableAdmission
    val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
    val dataContainerId = df.dataContainerID

    val lambda = 0.0

    val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
    val r = bigRClient.execute[Array[Array[String]]](splitter)
    assert(r.isSuccess)
    println(r.result)
    assert(r.result.length === 1)

    val alpha_length: Int = 100

    r.result.foreach {
      split ⇒
        val Array(train, test) = split
        val lambda = 0.0

        // fake the training with learningRate = 0.0
        val trainer = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
        val r = bigRClient.execute[IModel](trainer)
        assert(r.isSuccess)

        val persistenceID = r.result.getName

        val predictor = new YtrueYpred(dataContainerId, persistenceID)
        val r2 = bigRClient.execute[YtrueYpredResult](predictor)
        assert(r2.isSuccess)
        val predictionId = r2.result.dataContainerID

        val executor = new ROC(predictionId, alpha_length)
        val ret = bigRClient.execute[RocObject](executor)
        println(ret.toJson)
        assert(ret.isSuccess)
    }
  }

  test("Test ROC metric with CV random split on Shark") {
    createTableAdmission
    val df = this.runSQL2RDDCmd("select v3, v4, v1 from admission", true)
    val dataContainerId = df.dataContainerID

    val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
    val r = bigRClient.execute[Array[Array[String]]](splitter)
    assert(r.isSuccess)
    println(r.result)
    assert(r.result.length === 1)

    val alpha_length: Int = 100

    r.result.foreach {
      split ⇒
        val Array(train, test) = split
        val lambda = 0.0

        // fake the training with learningRate = 0.0
        val trainer = new LogisticRegression(dataContainerId, Array(0, 1), 2, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
        val r = bigRClient.execute[IModel](trainer)
        assert(r.isSuccess)
        val persistenceID = r.result.getName

        val predictor = new YtrueYpred(dataContainerId, persistenceID)
        val r2 = bigRClient.execute[YtrueYpredResult](predictor)
        assert(r2.isSuccess)
        val predictionId = r2.result.dataContainerID

        val executor = new ROC(predictionId, alpha_length)
        val ret = bigRClient.execute[RocMetric](executor)
        assert(ret.isSuccess)
    }
  }
}
