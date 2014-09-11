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
import com.adatao.spark.ddf.analytics.TModel
import io.ddf.types.Matrix
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.spark.ddf.analytics.ALossFunction
import com.adatao.spark.ddf.analytics.RDDImplicits._
import java.util.HashMap
import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.CompRowMatrix
import io.ddf.types.MatrixSparse
import org.jblas.MatrixFunctions
import scala.util.Random
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import io.ddf.DDFManager
import com.adatao.pa.spark.SparkThread
import com.adatao.pa.spark.types.ExecutorResult
import io.ddf.DDF
import io.ddf.exception.DDFException
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessResult

import com.adatao.spark.ddf.etl.TransformationHandler
import io.ddf.ml.IModel

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
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf: DDF = ddfManager.getDDF(ddfId)
    try {
      val xColsName = xCols.map { idx => ddf.getColumnName(idx) }
      val yColName = ddf.getColumnName(yCol)
      val transformedDDF = ddf.getTransformationHandler.asInstanceOf[TransformationHandler].dummyCoding(xColsName, yColName)


      val regressionModel = transformedDDF.ML.train("logisticRegressionCRS", 10: java.lang.Integer,
        0.1: java.lang.Double, 0.1: java.lang.Double, initialWeights.toArray: scala.Array[Double], columnsSummary)

      val rawModel = regressionModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.LogisticRegressionModel]
      if (ddf.getSchema().getDummyCoding() != null)
        rawModel.setMapping(ddf.getSchema().getDummyCoding().getMapping())

      regressionModel
    } catch {
      case ioe: DDFException ⇒ throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, ioe.getMessage(), null);
    }
  }
}

