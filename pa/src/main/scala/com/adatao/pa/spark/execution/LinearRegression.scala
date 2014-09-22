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

package com.adatao.pa.spark.execution

import java.lang.String

import io.spark.ddf.SparkDDF
import com.adatao.spark.ddf.etl.TransformationHandler
import com.adatao.spark.ddf.analytics._
import io.ddf.DDF
import io.ddf.ml.IModel
import com.adatao.spark.ddf.etl.TransformationHandler._
/**
 * Entry point for SparkThread executor
 */
class LinearRegression(
  dataContainerID: String,
  xCols: Array[Int],
  yCol: Int,
  var numIters: Int,
  var learningRate: Double,
  var ridgeLambda: Double,
  var initialWeights: Array[Double])
  extends AExecutor[IModel] {

  override def runImpl(context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager()
    val ddf = ddfManager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }

    val xColsName = xCols.map { idx => ddf.getColumnName(idx) }
    val yColName = ddf.getColumnName(yCol)
    val transformedDDF = ddf.getTransformationHandler.dummyCoding(xColsName, yColName)

    val model = transformedDDF.ML.train("linearRegressionWithGD", numIters: java.lang.Integer, learningRate: java.lang.Double, ridgeLambda: java.lang.Double,
      initialWeights)

    // converts DDF model to old PA model
    val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.LinearRegressionModel]
    if (transformedDDF.getSchema().getDummyCoding() != null)
      rawModel.setDummy(transformedDDF.getSchema().getDummyCoding())
    model
  }
}

//object LinearRegression {
//  /**
//   * As a client with our own data representation [[RDD(Matrix, Vector]], we need to supply our own LossFunction that
//   * knows how to handle that data.
//   *
//   * NB: We separate this class into a static (companion) object to avoid having Spark serialize too many unnecessary
//   * objects, if we were to place this class within [[class LinearRegression]].
//   */
//  class LossFunction(@transient XYData: RDD[(Matrix, Vector)], ridgeLambda: Double) extends ALinearGradientLossFunction(XYData, ridgeLambda) {
//    def compute: Vector => ALossFunction = {
//      (weights: Vector) => XYData.map { case (x, y) => this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
//    }
//  }
//}

