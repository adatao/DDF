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

import org.jblas.DoubleMatrix
import io.ddf.types.{Matrix, Vector}
import java.util.HashMap
import io.ddf.ml.IModel
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
    initialWeights: Vector
    ): LinearRegressionModel = {

    val numFeatures = XYData._1.getColumns()
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
}



