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
import com.adatao.spark.ddf.analytics._
import com.adatao.spark.ddf.analytics.Utils
import java.util.HashMap
import io.ddf.DDFManager
import io.ddf.DDF
import io.ddf.exception.DDFException
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.types.SuccessResult
import com.adatao.spark.ddf.etl.TransformationHandler
import io.ddf.ml.IModel
import com.adatao.spark.ddf.etl.TransformationHandler._

class LogisticRegressionCRSResult(model: LogisticRegressionModel) extends SuccessResult {
}

/**
 * Entry point for SparkThread executor
 */
class LogisticRegressionCRS(
  dataContainerID: String,
  xCols: Array[Int],
  yCol: Int,
  columnsSummary: HashMap[String, Array[Double]],
  var numIters: Int,
  var learningRate: Double,
  var ridgeLambda: Double,
  var initialWeights: Array[Double]) extends AExecutor[IModel] {

  var ddfManager: DDFManager = null

  //  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
  //	override def run(sparkThread: SparkThread): ExecutorResult = {
  override def runImpl(ctx: ExecutionContext): IModel = {
    ddfManager = ctx.sparkThread.getDDFManager();
    val ddf: DDF = ddfManager.getDDF(dataContainerID)
    try {
      val xColsName = xCols.map { idx => ddf.getColumnName(idx) }
      val yColName = ddf.getColumnName(yCol)
      val transformedDDF = ddf.getTransformationHandler.dummyCoding(xColsName, yColName)

      val regressionModel = transformedDDF.ML.train("logisticRegressionCRS", 10: java.lang.Integer,
        0.1: java.lang.Double, 0.1: java.lang.Double, initialWeights.toArray: scala.Array[Double], columnsSummary)

      val rawModel = regressionModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.LogisticRegressionModel]
      if (ddf.getSchema().getDummyCoding() != null)
        rawModel.setMapping(ddf.getSchema().getDummyCoding().getMapping())
      regressionModel
    } catch {
      case e: DDFException ⇒ e.printStackTrace(); throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage, e.getCause);
    }
  }
}

