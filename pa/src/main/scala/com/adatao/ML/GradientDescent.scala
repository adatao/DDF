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

package com.adatao.ML

import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector

/**
 * @author ctn
 *
 */
class GradientDescent {
}

object GradientDescent extends TCanLog {

	def runBatch[XYDataType](
		lossFunction: ALossFunction,
		initialWeights: Vector,
		learningRate: Double,
		numIters: Int): (Vector, Vector, Long) = {

		println(">>>>>>>>>>>>>>>> GradientDescent initial weights rows = " + initialWeights.getRows() + "\tnumColumns=" + initialWeights.getColumns())

		// Track training errors for all iterations, plus 1 more for the error before the first iteration
		val trainingLosses = new Vector(numIters + 1)
		val weights = initialWeights.dup

		var iter = 0
		var computedLoss: ALossFunction = null
		while (iter < numIters + 1) {
			
			computedLoss = lossFunction.compute(weights)
			
			// Update the weights, except for the last iteration
			// weights = weights - alpha * averageGradient
			if (iter <= numIters) weights.subi(computedLoss.gradients.mul(learningRate / computedLoss.numSamples))
			trainingLosses.put(iter, computedLoss.loss / computedLoss.numSamples)
			
			println(">>>>>> trainingLoss(" + iter + ")=" + trainingLosses.get(iter))

			iter += 1
		}

		
		//		LOG.info("final weights = %s".format(weights.toString))
		//		LOG.info("trainingLosses = %s".format(trainingLosses.toString))

		(weights, trainingLosses, computedLoss.numSamples)
	}
}
