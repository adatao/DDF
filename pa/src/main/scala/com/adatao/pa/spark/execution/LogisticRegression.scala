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

import com.adatao.ML
import com.adatao.ML.Utils
import com.adatao.ML.TModel
import io.ddf.types.Matrix
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.ML.LogisticRegressionModel
import com.adatao.ML.ALossFunction
import com.adatao.spark.RDDImplicits._
import java.util.HashMap
import java.util.List
import java.util.ArrayList

import io.ddf.DDF
import scala.collection.mutable.ArrayBuffer
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
  extends AExecutor[IModel]{

  override def runImpl(context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfId) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }

    val trainedColumns = (xCols :+ yCol).map(idx => ddf.getColumnName(idx))
    val projectDDF = ddf.VIEWS.project(trainedColumns: _*)

    //call dummy coding explicitly
    //make sure all input ddf to algorithm MUST have schema
    projectDDF.getSchemaHandler().computeFactorLevelsForAllStringColumns()
    projectDDF.getSchema().generateDummyCoding()

    var numFeatures: Integer = xCols.length + 1
    if (projectDDF.getSchema().getDummyCoding() != null)
      numFeatures = projectDDF.getSchema().getDummyCoding().getNumberFeatures

    LOG.info(">>>>>>>>>>>>>> LogisticRegressionIRLS numFeatures = " + numFeatures)

    val model = projectDDF.ML.train("logisticRegressionWithGD", numFeatures: java.lang.Integer, xCols, yCol: java.lang.Integer, numIters: java.lang.Integer, learningRate: java.lang.Double, ridgeLambda: java.lang.Double, initialWeights)

    val rawModel = model.getRawModel.asInstanceOf[com.adatao.ML.LogisticRegressionModel]
    if (projectDDF.getSchema().getDummyCoding() != null)
      rawModel.setMapping(projectDDF.getSchema().getDummyCoding().getMapping())
    model
  }

}

object LogisticRegression {
  /**
   * As a client with our own data representation [[RDD(Matrix, Vector]], we need to supply our own LossFunction that
   * knows how to handle that data.
   *
   * NB: We separate this class into a static (companion) object to avoid having Spark serialize too many unnecessary
   * objects, if we were to place this class within [[class LogisticRegression]].
   */
  class LossFunction(@transient XYData: RDD[(Matrix, Vector)], ridgeLambda: Double) extends ML.ALogisticGradientLossFunction(XYData, ridgeLambda) {
    def compute: Vector => ALossFunction = {
      (weights: Vector) => XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
    }
  }
}

/**
* Entry point for SparkThread executor to execute predictions
*/
class LogisticRegressionPredictor(val model: LogisticRegressionModel, val features: Array[Double]) extends APredictionExecutor[java.lang.Double] {
  def predict: java.lang.Double = model.predict(features).asInstanceOf[java.lang.Double]
}
