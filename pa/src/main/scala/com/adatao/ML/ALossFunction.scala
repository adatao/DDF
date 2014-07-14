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
 * @author ctn
 *
 */
package com.adatao.ML

import org.jblas.MatrixFunctions
import org.jblas.DoubleMatrix
import io.ddf.types.Matrix
import io.ddf.types.Vector

/**
 * Useful functions such as sigmoid() are defined here
 */

object ALossFunction {
	def sigmoid(z: DoubleMatrix): Vector = Vector(MatrixFunctions.expi(z.neg).addi(1.0)).reciprocal
	def sigmoid(z: Vector): Vector = Vector(MatrixFunctions.expi(z.neg).addi(1.0)).reciprocal
	def sigmoid(z: Double): Double = 1 / (math.exp(-z) + 1)

	/**
	 * Avoid over/underflow of log(sigmoid(x)) for very large values x < -710 or x > 710.
	 * We substitute overflowed valeus by appropriate given limits (given by caller).
	 */
	def safeLogOfSigmoid(hypothesis: DoubleMatrix, subtituteIfInfinity: DoubleMatrix) = {
		var result = MatrixFunctions.log(hypothesis)
		var i = 0
		while (i < hypothesis.length) {
			if (result.get(i).isInfinity) result.put(i, subtituteIfInfinity.get(i))
			i += 1
		}

		result
	}
}

/**
 * ALossFunction is a class with methods defined to compute loss surfaces
 * and their gradients.
 *
 * Note that the losses (J) are computed only for records and analysis.
 * If we wanted to be even faster we could skip computing losses altogether,
 * and focus only on the gradients needed to update the weights.
 */
abstract class ALossFunction extends Serializable {
	var gradients: Vector = _
	var loss: Double = 0
	var weights: Vector = _
	var numSamples: Long = 0

	/**
	 * Simply returns the client-provided XYData, if ever needed
	 */
	//	def getXYData: XYDataType = XYData

	/**
	 * Sum in-place to avoid new object alloc.
	 *
	 * May override this to define an associative-commutative summing function that
	 * aggregates multiple ALossFunction into one.
	 *
	 */
	def aggregate(other: ALossFunction): ALossFunction = {
		gradients.addi(other.gradients)
		loss += other.loss
		numSamples += other.numSamples
		this
	}

	/**
	 * Must override this to compute the gradient for a given X (features),Y (output) data table, and input weights.
	 *
	 * @param X - Matrix of input features. Row matrix of size [m x n] where m = number of samples, and n is number of features.
	 * @param Y - Output values.
	 * @param weights - Column vector containing weights for every feature.
	 */
	def compute(X: Matrix, Y: Vector, weights: Vector): ALossFunction

	/**
	 * Must override this to define a hyper-function that calls [[this.compute(X, Y, weights)]] while transforming
	 * the input data XYData: XYDataType into the appropriate X: Matrix and Y: Vector.
	 *
	 * This is the key to decoupling of the algorithm from the execution environment's data representation, since
	 * [[XYDataType]] is parameterized. Each implementation can provide its own [[ALossFunction]] that knows how to take
	 * source data of type [[XYDataType]], and provides a way to compute the loss function value out of it.
	 * See, e.g., [[com.adatao.ML.LinearRegression.LossFunction]] and compare it with
	 * [[com.adatao.pa.spark.execution.LinearRegression.LossFunction]].
	 */
	def compute: Vector ⇒ ALossFunction
}
