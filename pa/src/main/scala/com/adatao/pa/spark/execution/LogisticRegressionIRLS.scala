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
import com.adatao.ML.LogisticRegressionModel
import com.adatao.ML.ALossFunction
import com.adatao.spark.RDDImplicits._
import java.util.HashMap
import com.adatao.ML.ALinearModel
import com.adatao.ML.ADiscreteIterativeLinearModel
import com.adatao.ML.AContinuousIterativeLinearModel
import org.jblas.DoubleMatrix
import org.jblas.Solve
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.ddf.exception.DDFException
import com.adatao.ddf.DDF
import com.adatao.ddf.types.TupleMatrixVector
import scala.util.Random

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
		extends AModelTrainer[IRLSLogisticRegressionModel](dataContainerID, xCols, yCol, mapReferenceLevel) {

	override def runImpl(ctx: ExecutionContext): IRLSLogisticRegressionModel = {
		if (numIters == 0) // when client does not send numIters to this executor 
			numIters = 25
		if (eps == 0) // when client does not send eps to this executor 
			eps = 1e-8

		val ddfManager = ctx.sparkThread.getDDFManager();
		val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"))
		
		
		
		//call dummy coding explicitly
		//make sure all input ddf to algorithm MUST have schema
		ddf.getSchemaHandler().computeFactorLevelsForAllStringColumns()
		ddf.getSchema().generateDummyCoding()
		
		val numFeatures = ddf.getSchema().getDummyCoding().getNumberFeatures
		println(">>>>>>>>>>>>>> LogisticRegressionIRLS numFeatures = " + numFeatures)
		
		try {
			
			val regressionModel = ddf.ML.train("logisticRegressionIRLS", numFeatures: java.lang.Integer, numIters: java.lang.Integer, eps: java.lang.Double, ridgeLambda: java.lang.Double, initialWeights: scala.Array[Double], nullModel: java.lang.Boolean)

			//      val glmModel = ddfTrain3.ML.train("logisticRegressionCRS", 10: java.lang.Integer,
			//    0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray : scala.Array[Double], ddfTrain3.getNumColumns: java.lang.Integer, columnsSummary)

			val model: com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel = regressionModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel]

			println(">>>>>>>>>>>>>model=" + model)

			return new IRLSLogisticRegressionModel(model.getWeights, model.getDeviance, model.getNullDeviance, model.getNumSamples, ddf.getNumColumns(), model.getNumIters, model.getStdErrs)
		}
		catch {
			case ioe: DDFException ⇒ throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, ioe.getMessage(), null);
		}

	}

	override def train(dataContainerID: String, context: ExecutionContext): IRLSLogisticRegressionModel = {
		null.asInstanceOf[IRLSLogisticRegressionModel]
	}

	override def train(dataPartition: RDD[(Matrix, Vector)], context: ExecutionContext): IRLSLogisticRegressionModel = {
		null.asInstanceOf[IRLSLogisticRegressionModel]
	}

	//post process, set column mapping to model
	def instrumentModel(model: IRLSLogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]): IRLSLogisticRegressionModel = {
		null.asInstanceOf[IRLSLogisticRegressionModel]
	}
}

class IRLSLogisticRegressionModel(weights: Vector, val deviance: Double, val nullDeviance: Double, numSamples: Long, val numFeatures: Long, val numIters: Int, val stderrs: Vector) extends ALinearModel[Double](weights, numSamples) {
	override def predict(features: Vector): Double = ALossFunction.sigmoid(this.linearPredictor(features))
}
