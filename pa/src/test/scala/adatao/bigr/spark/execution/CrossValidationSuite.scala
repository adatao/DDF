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

package adatao.bigr.spark.execution

import adatao.ML
import adatao.ML.LogisticRegressionModel
import adatao.bigr.spark.types.ABigRClientTest
import org.junit.Assert._
import adatao.ML.LinearRegressionModel
import adatao.ML.spark.RocObject
import adatao.bigr.spark.types.ExecutionResult
/**
 *
 */
class CrossValidationSuite extends ABigRClientTest {

	private def loadTestFile: String = {
		this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		//		this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
	}

	test("CVRandomSplit") {
		val dataContainerId = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, " ")
		val splitter = new CVRandomSplit(dataContainerId, 5, 0.75, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 5)
		r.result.foreach { split ⇒
			assert(split.length === 2)
		}
	}

	test("CVKFoldSplit") {
		val dataContainerId = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, " ")
		val splitter = new CVKFoldSplit(dataContainerId, 5, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 5)
		r.result.foreach { split ⇒
			assert(split.length === 2)
		}
	}

	test("R2 score on CVRandomSplit") {
		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		val splitter = new CVRandomSplit(dataContainerId, 1, 0.75, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 1)
		r.result.foreach { split ⇒
			val Array(train, test) = split

			val trainer = new LinearRegression(train, Array(5), 0, 1, 0.0, 0.0, Array(37.285, -5.344))
			val r = bigRClient.execute[LinearRegressionModel](trainer)
			assert(r.isSuccess)

			val persistenceID = r.persistenceID

			val scorer = new R2Score(test, Array(5), 0, persistenceID)
			val r2 = bigRClient.execute[Double](scorer)
			assert(r2.isSuccess)

			println("!!! AHT !!! " + r2.result)
			// no assertion here because result are dependent on random draw
			// though deterministic given the seed
		}
	}

	test("R2 score on CVRandomSplit on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		val dataContainerId = r0.result.dataContainerID

		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 1)
		r.result.foreach { split ⇒
			val Array(train, test) = split

			// fake the training with learningRate = 0.0
			val trainer = new LinearRegression(train, Array(5), 0, 1, 0.0, 0.0, Array(37.285, -5.344))
			val r = bigRClient.execute[LinearRegressionModel](trainer)
			assert(r.isSuccess)

			val persistenceID = r.persistenceID

			val scorer = new R2Score(test, Array(5), 0, persistenceID)
			val r2 = bigRClient.execute[Double](scorer)
			assert(r2.isSuccess)

			println("!!! AHT !!! " + r2.result)
			// no assertion here because result are dependent on random draw
			// though deterministic given the seed
		}
	}

	test("LinearRegression on CVRandomSplit on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		val dataContainerId = r0.result.dataContainerID

		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 1)
		r.result.foreach { split ⇒
			val Array(train, test) = split

			// fake the training with learningRate = 0.0
			val trainer = new LinearRegressionNormalEquation(train, Array(5), 0, 0.0)
			val r = bigRClient.execute[LinearRegressionModel](trainer)
			assert(r.isSuccess)
		}
	}

	test("Test ROC metric with CV random split") {
		val dataContainerId = this.loadFile(List("resources/admission.csv", "server/resources/admission.csv"), false, " ")
		val lambda = 0.0

		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 1)

		val alpha_length: Int = 100

		r.result.foreach { split ⇒
			val Array(train, test) = split
			val lambda = 0.0

			// fake the training with learningRate = 0.0
			val trainer = new LogisticRegression(dataContainerId, Array(2, 3), 0, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
			val r = bigRClient.execute[LogisticRegressionModel](trainer)
			assert(r.isSuccess)

			val persistenceID = r.persistenceID

			val predictor = new YtrueYpred(dataContainerId, persistenceID, Array(2, 3), 0)
			val r2 = bigRClient.execute[YtrueYpredResult](predictor)
			assert(r2.isSuccess)
			val predictionId = r2.result.dataContainerID

			val executor = new ROC(predictionId, Array(0, 1), alpha_length)
			val ret = bigRClient.execute[RocObject](executor)
			println(ret.toJson)
			assert(ret.isSuccess)
		}
	}

	test("Test ROC metric with CV random split on Shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		val dataContainerId = r0.result.dataContainerID

		val splitter = new CVRandomSplit(dataContainerId, 1, 0.5, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 1)

		val alpha_length: Int = 100

		r.result.foreach { split ⇒
			val Array(train, test) = split
			val lambda = 0.0

			// fake the training with learningRate = 0.0
			val trainer = new LogisticRegression(dataContainerId, Array(4, 5), 7, 1, 0.0, lambda, Array(-3.0, 1.5, -0.9))
			val r = bigRClient.execute[LogisticRegressionModel](trainer)
			assert(r.isSuccess)
			val persistenceID = r.persistenceID

			val predictor = new YtrueYpred(dataContainerId, persistenceID, Array(2, 3), 0)
			val r2 = bigRClient.execute[YtrueYpredResult](predictor)
			assert(r2.isSuccess)
			val predictionId = r2.result.dataContainerID

			val executor = new ROC(predictionId, Array(0, 1), alpha_length)
			val ret = bigRClient.execute[RocObject](executor)
			assert(ret.isSuccess)

		}
	}
}
