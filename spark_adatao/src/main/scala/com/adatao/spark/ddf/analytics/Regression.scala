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

package com.adatao.spark.ddf.analytics
import scala.util.Random
import com.adatao.spark.ddf.analytics.ALossFunction
import io.ddf.types.Matrix
import io.ddf.types.Vector

/**
 * Companion object needed to interface with clients.
 */
object Regression {
	/**
	 * XYDataType is parameterized in order to support different types of distributed data representations
	 */
	def train[XYDataType](
		lossFunction: ALossFunction,
		numIters: Int, learningRate: Double,
		initialWeights: Vector,
		numFeatures: Int /* only used if initialWeights is null */)(implicit m: Manifest[XYDataType]): (Vector, Vector, Long) = {

		new Regression(numIters, learningRate).train(lossFunction, initialWeights, numFeatures)
	}
}

class Regression[XYDataType](numIters: Int, learningRate: Double)(implicit m: Manifest[XYDataType])
		extends TAlgorithm {

	// Generate random weights if necessary
	private def getWeights(initialWeights: Option[Vector], numFeatures: Int /* used only if initialWeights is null */ ): Vector = {
		initialWeights match {
			case Some(vector) ⇒ vector
			case None ⇒ Vector(Seq.fill(numFeatures)(Random.nextDouble).toArray)
		}
	}

	/**
	 * train() uses the Strategy pattern to perform gradient-descent optimization:
	 * <ul>
	 * <li>`GradientDescent.runBatch()` knows how to iterate through numIters gradient-descent steps</li>
	 * <li>With each step, it uses the supplied lossFunction to compute losses and gradients</li>
	 * </ul>
	 */
	def train(
		lossFunction: ALossFunction,
		initialWeights: Vector,
		numFeatures: Int /* used only if initialWeights is null */ )(implicit m: Manifest[XYDataType]): (Vector, Vector, Long) = {

		val (finalWeights, trainingErrors, numSamples) = GradientDescent.runBatch(lossFunction, getWeights(Option(initialWeights), numFeatures), learningRate, numIters)
		(finalWeights, trainingErrors, numSamples)
	}
}
