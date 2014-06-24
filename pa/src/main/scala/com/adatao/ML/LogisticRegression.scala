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

import java.lang.String
import java.util.Arrays
import org.jblas.DoubleMatrix
import scala.util.Random
import org.jblas.MatrixFunctions
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import java.util.HashMap
import com.adatao.ddf.ml.IModel
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
    LOG.info(">>>>>>>>>>>>>>>. calling predict")
    ALossFunction.sigmoid(this.linearPredictor(features))
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

/**
 * A base for LogisticGradientLossFunctions supporting different XYDataTypes.
 * We don't provide the compute() hyper-function, because derived classes have to provide
 * the implementation that's unique to each XYDataType. For an example implementation, see
 * com.adatao.ML.LogisticGradientLossFunction.
 */
abstract class ALogisticGradientLossFunction[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
		extends ALinearGradientLossFunction[XYDataType](XYData, ridgeLambda) {

	/**
	 * Override to apply the sigmoid function
	 *
	 * hypothesis[vector] = sigmoid(weights*X)
	 */
	override def computeHypothesis(X: Matrix, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
		val linearPredictor = this.computeLinearPredictor(X, weights)
		(linearPredictor, ALossFunction.sigmoid(linearPredictor))
	}

	// LogisticRegression gradients is exactly the same as LinearRegression's

	/**
	 * Override to compute the appropriate loss function for logistic regression
	 *
	 * h = hypothesis
	 * J[scalar] = -(Y'*log(h) + (1-Y')*log(1-h)) + (lambda*weights^2 / 2)
	 */
	override def computeLoss(X: Matrix, Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix) = {
		/**
		 * We have
		 *   a1. lim log(sigmoid(x)) = 0 as x goes to +infinity
		 *   a2. lim log(sigmoid(x)) = -x as x goes to -infinity
		 * Likewise, 
		 *   b1. lim log(1-sigmoid(x)) = -x as x goes to +infinity
		 *   b2. lim log(1-sigmoid(x)) = 0 as x goes to -infinity
		 *
		 * We calculate h = sigmoid(x) under floating point arithmetic,
		 * then we calculate log(h) and 1-log(h), substituting overflowed values.
		 *
		 * The behavior of cases a1 and b2  hold under floating point arithmetic, i.e.
		 *   a1. when x > 720, h = 1, log h = 0
		 *   b2. when x < -720, h = 0, 1-h) = 1, log (1-h) = 0
		 * The other two cases result in overflow:
		 *   a2. when x < -720, h = 0, log h = -Infinity => replace with x
		 *   b1. when x > 720, h = 1, (1-h) = 0, log (1-h) = -Infinity => replace with -x
		 *
		 * This is actually quite complicated for a misleadingly-simple-looking few lines of code.
		 */
		val YT = Y.transpose()
		val lossA: Double = Y.dot(ALossFunction.safeLogOfSigmoid(hypothesis, linearPredictor)) // Y' x log(h)
		val lossB: Double = Vector.fill(Y.length, 1.0).subi(Y).dot(
			ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), linearPredictor.neg)
		) // (1-Y') x log(1-h)
		var J = -(lossA + lossB)
		if (ridgeLambda != 0.0) J += (ridgeLambda / 2) * weights.dot(weights)
		J
	}
}


