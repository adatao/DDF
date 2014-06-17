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

import org.jblas.DoubleMatrix
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import java.util.HashMap
import com.adatao.ddf.ml.IModel
import java.util.Arrays
/**
 * Companion object to provide friendly-name access to clients.
 */
object LinearRegression {

  /**
   * This is the signature to be used by clients that can represent their data using [[Matrix]] and [[Vector]]
   */
  def train(
    XYData: (Matrix, Vector),
    numIters: Int,
    learningRate: Double,
    ridgeLambda: Double,
    initialWeights: Vector,
    numFeatures: Int): LinearRegressionModel = {

    this.train(new LinearRegression.LossFunction(XYData, ridgeLambda), numIters, learningRate, initialWeights, numFeatures)
  }

  /**
   * This is the signature to be used by clients wishing to inject their own loss function that can handle their own
   * data representation (e.g., [[spark.RDD]]).
   */
  def train[XYDataType](
    lossFunction: ALinearGradientLossFunction[XYDataType],
    numIters: Int,
    learningRate: Double,
    initialWeights: Vector,
    numFeatures: Int)(implicit m: Manifest[XYDataType]): LinearRegressionModel = {

    val (weights, trainingLosses, numSamples) = Regression.train(lossFunction, numIters, learningRate, initialWeights, numFeatures)
    new LinearRegressionModel(weights, trainingLosses, numSamples)
  }

  /**
   * Provide computeFunction supporting (Matrix, Vector) XYDataType to GradientDescent computer
   */
  class LossFunction(XYData: (Matrix, Vector), ridgeLambda: Double) extends ALinearGradientLossFunction(XYData, ridgeLambda) {
    override def compute: Vector ⇒ ALossFunction = {
      (weights: Vector) ⇒ this.compute(XYData._1, XYData._2, weights)
    }
  }
}

class LinearRegressionModel(weights: Vector, trainingLosses: Vector, numSamples: Long) extends AContinuousIterativeLinearModel(weights, trainingLosses, numSamples) {
  // The base class already sufficiently implements the predictor as [weights DOT features]
  @transient var ddfModel: IModel = null
  override def ddfModelID: String = {
    if (ddfModel != null) ddfModel.getName()
    else null
  }
  
  def setMapping(_mapping: HashMap[Integer, HashMap[String, java.lang.Double]]) {
    dummyColumnMapping = _mapping
  }
}

/**
 * A base for LinearGradientLossFunctions supporting different XYDataTypes.
 * We don't provide the compute() hyper-function, because derived classes have to provide
 * the implementation that's unique to each XYDataType. For an example implementation, see
 * com.adatao.ML.LinearGradientLossFunction.
 */
abstract class ALinearGradientLossFunction[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
  extends ALossFunction {

  final def computeLinearPredictor(X: Matrix, weights: Vector): DoubleMatrix = X.mmul(weights)

  /**
   * May override this to define a hypothesis function. The base implementation is
   *
   * hypothesis[vector] = weights*X
   *
   * @returns - tuple of (linearPredictor, hypothesis), as they may be different
   */
  protected def computeHypothesis(X: Matrix, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
    val linearPredictor = this.computeLinearPredictor(X, weights)
    (linearPredictor, linearPredictor)
  }

  /**
   * May override this to define a specific gradient (dJ/dWeights). The base implementation
   * computes the linear gradients with ridge regularization.
   *
   * errors = hypothesis - Y
   * totalGradients[vector] = errors*X + lambda*weights
   */
  protected def computeGradients(X: Matrix, weights: Vector, errors: DoubleMatrix): Vector = {
    val gradients = Vector(errors.transpose().mmul(X)) // (h - Y) x X = errors.transpose[1 x m] * X[m x n] = [1 x n] => Vector[n]
    if (ridgeLambda != 0.0) gradients.addi(weights.mul(ridgeLambda)) // regularization term, (h - Y) x X + L*weights
    gradients
  }

  /**
   * May override this to define a specific loss (J) function. The base implementation
   * computes the linear loss function with ridge regularization.
   *
   * J[scalar] = errors^2 + lambda*weights^2
   */
  protected def computeLoss(X: Matrix, Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix): Double = {
    var J = errors.dot(errors)
    if (ridgeLambda != 0.0) J += ridgeLambda * weights.dot(weights)
    J / 2
  }

  /**
   * Note that the losses are computed only for records and analysis. If we wanted to be even faster
   * we could skip computing losses altogether.
   */
  override def compute(X: Matrix, Y: Vector, theWeights: Vector): ALossFunction = {
    val (linearPredictor, hypothesis) = this.computeHypothesis(X, theWeights)
    val errors = hypothesis.sub(Y)
    gradients = this.computeGradients(X, theWeights, errors)
    loss = this.computeLoss(X, Y, theWeights, errors, linearPredictor, hypothesis)
    weights = theWeights
    numSamples = Y.rows

    this
  }
}

class RocObject(var pred: Array[Array[Double]], var auc: Double) extends LinearRegressionModel(new Vector(1), new Vector(1), 0) {

  def print() {
    var i: Int = 0
    while (i < this.pred.length) {
      if (this.pred(i) != null) {
      }
      i = i + 1
    }
  }
  def computeAUC(): Double = {
    //filter null/NA in pred
    var i: Int = 0
    var previousTpr: Double = 0
    var previousFpr: Double = 0
    i = pred.length - 1
    while (i >= 0) {
      //0 index is for threshold value
      if (pred(i) != null) {
        //accumulate auc
        auc = auc + previousTpr * (pred(i)(2) - previousFpr) + 0.5 * (pred(i)(2) - previousFpr) * (pred(i)(1) - previousTpr)
        //update previous
        previousTpr = pred(i)(1)
        previousFpr = pred(i)(2)
      }
      i = i - 1
    }
    auc
  }

  override def toString: String = {
    "RocObject: pred = %.4f\t".format(
      Arrays.toString(pred(0)))
  }
  /**
   * Sum in-place to avoid new object alloc
   */
  def addIn(other: RocObject): RocObject = {
    // Sum tpr, fpr for each threshold
    var i: Int = 0
    //start from 1, 0-index is for threshold value
    var j: Int = 1
    while (i < this.pred.length) {
      if (this.pred(i) != null) {
        if (other.pred(i) != null) {
          j = 1
          //P = P + P
          //N = N + N
          while (j < this.pred(i).length) {
            this.pred(i)(j) = this.pred(i)(j) + other.pred(i)(j)
            j = j + 1
          }
        }
      } else {
        if (other.pred(i) != null) {
          j = 0
          //P = P + P
          //N = N + N
          //this.pred(i) is currently null so need to cretae new instance
          this.pred(i) = new Array[Double](3)
          while (j < other.pred(i).length) {
            this.pred(i)(j) = other.pred(i)(j)
            j = j + 1
          }
        }
      }
      i = i + 1
    }
    this
  }
}


