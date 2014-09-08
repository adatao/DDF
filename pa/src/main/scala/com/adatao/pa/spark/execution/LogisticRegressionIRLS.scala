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
import com.adatao.spark.ddf.analytics.TModel
import io.ddf.types.Matrix
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import com.adatao.spark.ddf.analytics.ALossFunction
import com.adatao.spark.ddf.analytics.RDDImplicits._
import java.util.HashMap
import com.adatao.spark.ddf.analytics.ALinearModel
import com.adatao.spark.ddf.analytics.ADiscreteIterativeLinearModel
import com.adatao.spark.ddf.analytics.AContinuousIterativeLinearModel
import org.jblas.DoubleMatrix
import org.jblas.Solve
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import io.ddf.exception.DDFException
import io.ddf.DDF
import io.ddf.types.TupleMatrixVector
import scala.util.Random
import io.ddf.ml.IModel
import com.adatao.spark.ddf.etl.TransformationHandler

/**
 * NhanVLC
 * Logistic Regression using Iterative Re-weighted Least Square
 * Refs:
 * http://komarix.org/ac/papers/thesis/thesis_html/node20.html
 * http://www.omidrouhani.com/research/logisticregression/html/logisticregression.htm#_Toc147483467
 * http://doc.madlib.net/v0.6/group__grp__logreg.html
 * http://www.win-vector.com/blog/2011/09/the-simpler-derivation-of-logistic-regression/
 */
class LogisticRegressionIRLS(
  dataContainerID: String,
  xCols: Array[Int],
  yCol: Int,
  var numIters: Int,
  var eps: Double,
  var ridgeLambda: Double,
  var initialWeights: Array[Double],
  mapReferenceLevel: HashMap[String, String] = null, nullModel: Boolean = false)
  extends AExecutor[IModel] {

  override def runImpl(ctx: ExecutionContext): IModel = {
    if (numIters == 0) // when client does not send numIters to this executor 
      numIters = 25
    if (eps == 0) // when client does not send eps to this executor 
      eps = 1e-8

    val ddfManager = ctx.sparkThread.getDDFManager();
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf: DDF = ddfManager.getDDF(ddfId)

    val xColsName = xCols.map { idx => ddf.getColumnName(idx) }
    val yColName = ddf.getColumnName(yCol)
    val transformedDDF = ddf.getTransformationHandler.asInstanceOf[TransformationHandler].dummyCoding(xColsName, yColName)

    //including bias term or intercept
    var numFeatures: Integer = xCols.length + 1
    if (ddf.getSchema().getDummyCoding() != null)
      numFeatures = ddf.getSchema().getDummyCoding().getNumberFeatures

    try {
      val regressionModel = transformedDDF.ML.train("logisticRegressionIRLS", numFeatures: java.lang.Integer, numIters: java.lang.Integer, eps: java.lang.Double, ridgeLambda: java.lang.Double, initialWeights: scala.Array[Double], nullModel: java.lang.Boolean)
      val model: com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel = regressionModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel]

      if (ddf.getSchema().getDummyCoding() != null)
        model.setMapping(ddf.getSchema().getDummyCoding().getMapping())

      regressionModel
    } catch {
      case e: DDFException ⇒ throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}

