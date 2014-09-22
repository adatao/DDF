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
import com.adatao.spark.ddf.analytics.Utils
import com.adatao.spark.ddf.etl.TransformationHandler._
import io.ddf.DDF
import io.ddf.ml.IModel

/**
 * Entry point for SparkThread executor
 */
class LogisticRegression(
  dataContainerID: String,
  xCols: Array[Int],
  yCol: Int,
  var numIters: Int,
  var learningRate: Double,
  var ridgeLambda: Double,
  var initialWeights: Array[Double])
  extends AExecutor[IModel] {

  override def runImpl(context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddf = ddfManager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }

    val xColsName = xCols.map { idx => ddf.getColumnName(idx) }
    val yColName = ddf.getColumnName(yCol)
    val transformedDDF = ddf.getTransformationHandler.dummyCoding(xColsName, yColName)
    val model = transformedDDF.ML.train("logisticRegressionWithGD", numIters: java.lang.Integer,
      learningRate: java.lang.Double, ridgeLambda: java.lang.Double, initialWeights)

    val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.LogisticRegressionModel]
    if (ddf.getSchema().getDummyCoding() != null)
      rawModel.setDummy(ddf.getSchema().getDummyCoding())
    model
  }

}

//object LogisticRegression {
//  /**
//   * As a client with our own data representation [[RDD(Matrix, Vector]], we need to supply our own LossFunction that
//   * knows how to handle that data.
//   *
//   * NB: We separate this class into a static (companion) object to avoid having Spark serialize too many unnecessary
//   * objects, if we were to place this class within [[class LogisticRegression]].
//   */
//  class LossFunction(@transient XYData: RDD[(Matrix, Vector)], ridgeLambda: Double) extends ALogisticGradientLossFunction(XYData, ridgeLambda) {
//    def compute: Vector => ALossFunction = {
//      (weights: Vector) => XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
//    }
//  }
//}

