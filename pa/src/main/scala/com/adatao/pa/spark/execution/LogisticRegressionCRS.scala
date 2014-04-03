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
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.ML.ALossFunction
import com.adatao.spark.RDDImplicits._
import java.util.HashMap
import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.CompRowMatrix
import com.adatao.ddf.types.MatrixSparse
import org.jblas.MatrixFunctions
import com.adatao.ML.GradientDescent
import scala.util.Random
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import com.adatao.ddf.DDFManager
import com.adatao.pa.spark.SparkThread
import com.adatao.pa.spark.types.ExecutorResult
import com.adatao.ddf.DDF
import com.adatao.ddf.exception.DDFException
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessResult
import com.adatao.ddf.ml.IModel
import com.adatao.ML.LogisticRegressionModel

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
		var initialWeights: Array[Double]) extends AModelTrainer[LogisticRegressionModel](dataContainerID, xCols, yCol) {

	var ddfManager: DDFManager = null

	//  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
	//	override def run(sparkThread: SparkThread): ExecutorResult = {
	override def runImpl(ctx: ExecutionContext): LogisticRegressionModel = {

		ddfManager = ctx.sparkThread.getDDFManager();
		val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"))
		try {

			val regressionModel = ddf.ML.train("logisticRegressionCRS", 10: java.lang.Integer,
				0.1: java.lang.Double, 0.1: java.lang.Double, initialWeights.toArray: scala.Array[Double], xCols.size: java.lang.Integer, columnsSummary)

			val model: com.adatao.spark.ddf.analytics.LogisticRegressionModel = regressionModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.LogisticRegressionModel]
			val glm = new LogisticRegressionModel(model.getWeights, model.getTrainingLosses(), model.getNumSamples())
			return (glm)
		}
		catch {
			case ioe: DDFException ⇒ throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, ioe.getMessage(), null);
		}
	}

	override def train(dataContainerID: String, context: ExecutionContext): LogisticRegressionModel = {
		null.asInstanceOf[LogisticRegressionModel]
	}

	override def train(dataPartition: RDD[(Matrix, Vector)], context: ExecutionContext): LogisticRegressionModel = {
		null.asInstanceOf[LogisticRegressionModel]
	}

	override def instrumentModel(model: LogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]): LogisticRegressionModel = {
		null.asInstanceOf[LogisticRegressionModel]
	}

}

