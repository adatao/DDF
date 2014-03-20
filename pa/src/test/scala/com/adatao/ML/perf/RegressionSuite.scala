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
package com.adatao.ML.perf

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.ML.AAlgorithmTest
import com.adatao.ML.LinearRegression
import com.adatao.ML.LogisticRegression
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.ML.ATimedAlgorithmTest

/**
 * @author ctn
 *
 * Good perf benchmarking slides: https://speakerdeck.com/ichoran/designing-for-performance-scala-days-2013
 *
 */
class RegressionSuite extends ATimedAlgorithmTest {

	private val doRunTests = false	
	private val testOrIgnore = if (doRunTests) test _ else ignore _

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

	val lambda = 1.0
	val numReps = 100

	testOrIgnore("Single-variable linear regression 1") {
		val numIters = 1
		(1 to numReps).foreach(x ⇒ LinearRegression.train(XYData, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable linear regression 100") {
		val numIters = 100
		(1 to numReps).foreach(x ⇒ LinearRegression.train(XYData, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable linear regression 1000") {
		val numIters = 1000
		(1 to numReps).foreach(x ⇒ LinearRegression.train(XYData, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable linear regression 10000") {
		val numIters = 10000
		(1 to numReps).foreach(x ⇒ LinearRegression.train(XYData, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable logistic regression 1") {
		val numIters = 1
		(1 to numReps).foreach(x ⇒ LogisticRegression.train(XY2Data, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable logistic regression 100") {
		val numIters = 100
		(1 to numReps).foreach(x ⇒ LogisticRegression.train(XY2Data, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable logistic regression 1000") {
		val numIters = 1000
		(1 to numReps).foreach(x ⇒ LogisticRegression.train(XY2Data, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

	testOrIgnore("Single-variable logistic regression 10000") {
		val numIters = 10000
		(1 to numReps).foreach(x ⇒ LogisticRegression.train(XY2Data, numIters, 0.05, lambda, Vector(Array(0.0, 0.0)), 0))
	}

}
