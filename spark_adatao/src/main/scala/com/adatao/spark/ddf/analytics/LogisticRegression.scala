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

import java.lang.String
import java.util.Arrays
import org.jblas.DoubleMatrix
import scala.util.Random
import org.jblas.MatrixFunctions
import io.ddf.types.{Matrix, Vector}
import java.util.HashMap
import io.ddf.ml.IModel
import org.apache.spark.rdd.RDD

/**
 * Companion object to provide friendly-name access to clients.
 *
 * @param XYData - Training data with Y-values assumed to be either 0 or 1 in the classification sense
 */
object LogisticRegression {

	/**
	 * This is the signature to be used by clients that can represent their data using [[Matrix]] and [[Vector]]
	 */
	def train(
		XYData: (Matrix, Vector),
		numIters: Int,
		learningRate: Double,
		ridgeLambda: Double,
		initialWeights: Vector,
		numFeatures: Int): LogisticRegressionModel = {

		this.train(new LogisticRegression.LossFunction(XYData, ridgeLambda), numIters, learningRate, initialWeights, numFeatures)
	}

	/**
	 * This is the signature to be used by clients wishing to inject their own loss function that can handle their own
	 * data representation (e.g., [[spark.RDD]]).
	 */
	def train[XYDataType](
		lossFunction: ALogisticGradientLossFunction[XYDataType],
		numIters: Int,
		learningRate: Double,
		initialWeights: Vector,
		numFeatures: Int)(implicit m: Manifest[XYDataType]): LogisticRegressionModel = {

		val (weights, trainingLosses, numSamples) = Regression.train(lossFunction, numIters, learningRate, initialWeights, numFeatures)
		new LogisticRegressionModel(weights=weights, trainingLosses= trainingLosses, numSamples= numSamples)
	}

	/**
	 * Provide compute supporting (Matrix, Vector) XYDataType to GradientDescent computer
	 */
	class LossFunction(XYData: (Matrix, Vector), ridgeLambda: Double) extends ALogisticGradientLossFunction(XYData, ridgeLambda) {
		override def compute: Vector ⇒ ALossFunction = {
			(weights: Vector) ⇒ this.compute(XYData._1, XYData._2, weights)
		}
	}
	
	// batch prediction on a feature-extracted RDD[(Matrix, Vector)]
  def yTrueYpred[T <: TPredictiveModel[Vector, Double]](model: T, xyRDD: RDD[(Matrix, Vector)]): RDD[(Double, Double)] = {
    xyRDD.flatMap { xy ⇒
      xy match {
        case (x, y) ⇒ for (i ← 0 until y.size) yield (y(i), model.predict(Vector(x.getRow(i))))
      }
    }
  }
}


class LogisticRegressionModel(weights: Vector, trainingLosses: Vector, numSamples: Long) extends AContinuousIterativeLinearModel(weights, trainingLosses, numSamples) {

  override def predict(features: Vector): Double = {
    ALossFunction.sigmoid(this.linearPredictor(Vector(Array[Double](1) ++ features.data)))
  }

  override def predict(features: Array[Double]): Double = {
    this.predict(Vector(features))
  }

  def setMapping(_mapping: HashMap[Integer, HashMap[String, java.lang.Double]]) {
    dummyColumnMapping = _mapping
  }
}

class DiscreteLogisticRegressionModel(weights: Vector, trainingLosses: Vector, numSamples: Long) extends ADiscreteIterativeLinearModel(weights, trainingLosses, numSamples) {
	override def predict(features: Vector): Int = if (ALossFunction.sigmoid(this.linearPredictor(features)) < 0.5) 0 else 1
}



