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
package adatao.bigr.spark.execution.perf

import adatao.ML
import adatao.bigr.spark.execution._
import adatao.bigr.spark.types.ABigRClientTest
import adatao.ML.ATimedAlgorithmTest
import adatao.ML.LinearRegressionModel
import adatao.bigr.spark.types.ExecutionResult

/**
 * Tests for regression algorithms, having to do with the BigR environment
 *
 * @author ctn
 *
 */
class RegressionSuite extends ABigRClientTest {
	
	private val doRunTests = false
	private val testOrIgnore = if (doRunTests) test _ else ignore _
	
	def numIters = 300
	def lambda = 1.0

	private def loadTestFile: String = {
		// this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
	}

  testOrIgnore("Single-variable linear regression") {
		val executor = new LinearRegression(this.loadTestFile, Array(5), 4, numIters, 0.05, lambda, Array(38, -3))
		val r = bigRClient.execute[LinearRegressionModel](executor)

		assert(r.isSuccess)

		LOG.info("result is %s".format(r.toString))
	}
}
