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
package com.adatao.ML

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import io.ddf.types.Matrix
import io.ddf.types.Vector
import com.adatao.spark.ddf.analytics._
import com.adatao.spark.ddf.analytics.LinearRegression
import com.adatao.spark.ddf.analytics.LogisticRegression

/**
 * @author ctn
 *
 */
class RegressionSuite extends AAlgorithmTest {

	private val numIters = 100
	private val learningRate = 0.05
	private val initialWeights = Vector(Array(0.0, 0.0))
	private val numFeatures = initialWeights.length // doesn't matter when initialWeights is provided

	private val X = new Matrix(
		Array(
			Array(1.0, 0.0),
			Array(1.0, 1.0),
			Array(1.0, 2.0),
			Array(1.0, 3.0),
			Array(1.0, 4.0),
			Array(1.0, 5.0),
			Array(1.0, 6.0),
			Array(1.0, 8.0),
			Array(1.0, 9.0),
			Array(1.0, 10.0)
		)
	)

	private val Y = Vector(
		Array(1.0, 3.0, 5.0, 9.0, 10.0, 18.0, 19.0, 20.0, 27.0, 32.0)
	)

	private val Y2 = Vector(
		Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0)
	)

	private val XYData = (X, Y)

	private val XY2Data = (X, Y2)

	test("Single-variable linear regression") {
		val lambda = 0.0
		val model = LinearRegression.train(XYData, numIters, learningRate, lambda, initialWeights)

		assert(model != null)
		assert(model.weights != null)
		assert(model.trainingLosses != null)
		assert(truncate(model.weights(0), 2) === 0.12)
		assert(truncate(model.weights(1), 2) === 2.98)
		assert(truncate(model.trainingLosses(0), 2) === 152.7)
		assert(truncate(model.trainingLosses(1), 2) === 78.87)
	}

	test("Single-variable linear regression with regularization") {
		val lambda = 1.0
		val model = LinearRegression.train(XYData, numIters, learningRate, lambda, initialWeights)

		assert(model != null)
		assert(model.weights != null)
		assert(model.trainingLosses != null)
		assert(truncate(model.weights(0), 2) === 0.17)
		assert(truncate(model.weights(1), 2) === 2.96)
		assert(truncate(model.trainingLosses(0), 2) === 152.7)
		assert(truncate(model.trainingLosses(1), 2) === 80.16)
	}

	test("Single-variable logistic regression") {
		val lambda = 0.0
		val model = LogisticRegression.train(XY2Data, numIters, learningRate, lambda, initialWeights)

		assert(model != null)
		assert(model.weights != null)
		assert(model.trainingLosses != null)
		assert(truncate(model.weights(0), 2) === 0.77)
		assert(truncate(model.weights(1), 2) === -0.1)
		assert(truncate(model.trainingLosses(0), 2) === 0.69)
		assert(truncate(model.trainingLosses(1), 2) === 0.69)
	}

	test("Single-variable logistic regression with regularization") {
		val lambda = 1.0
		val model = LogisticRegression.train(XY2Data, numIters, learningRate, lambda, initialWeights)

		assert(model != null)
		assert(model.weights != null)
		assert(model.trainingLosses != null)
		assert(truncate(model.weights(0), 2) === 0.61)
		assert(truncate(model.weights(1), 2) === -0.07)
		assert(truncate(model.trainingLosses(0), 2) === 0.69)
		assert(truncate(model.trainingLosses(1), 2) === 0.69)
	}
}
