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
import com.adatao.ML.types.Matrix
import com.adatao.ML.types.Vector
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
	val initialWeights: Array[Double],
	mapReferenceLevel: HashMap[String, String] = null, nullModel: Boolean = false)
		extends AModelTrainer[IRLSLogisticRegressionModel](dataContainerID, xCols, yCol, mapReferenceLevel) {

	/**
	 * m:(r x c); d: (r x 1)
	 * We transpose m and multiply with diagonal matrix with d is the diagonal
	 */
	def tMmulDiagonal(m: DoubleMatrix, d: DoubleMatrix): DoubleMatrix = {
		val numRows = m.getRows
		val numCols = m.getColumns
		val ret = DoubleMatrix.ones(numCols, numRows)

		var c = 0
		while (c < numCols) {
			var r = 0
			while (r < numRows) {
				val m_val = m.get(r, c)
				ret.put(c, r, m_val * d.get(r, 0))
				r += 1
			}
			c += 1
		}
		ret
	}

	/**
	 * d: (r x 1): array of diagonal elements
	 * m:(r x c)
	 * Multifly d with m and put result back to m
	 */
	def diagonalMmul(d: DoubleMatrix, m: DoubleMatrix): DoubleMatrix = {
		val numRows = m.getRows
		val numCols = m.getColumns

		var r = 0
		while (r < numRows) {
			var c = 0
			while (c < numCols) {
				m.put(r, c, m.get(r, c) * d.get(r, 0))
				c += 1
			}
			r += 1
		}
		m
	}

	def handlePartition(lFunc: LogisticRegression.LossFunction, weights: Vector)(inputRows: Iterator[(Matrix, Vector)]): Iterator[(DoubleMatrix, DoubleMatrix, Long)] = {

		var XtWX: DoubleMatrix = null
		var XtWz: DoubleMatrix = null
		var nRows: Long = 0
		
		while (inputRows.hasNext && nRows==0) {
			inputRows.next match {
				case (x, y) ⇒ {
					if (x.columns == 0 && y.columns == 0) {
						XtWX = DoubleMatrix.zeros(weights.length, weights.length)
						XtWz = DoubleMatrix.zeros(weights.length, 1)
					}
					else {

						// calculate number of rows
						nRows = x.getRows.toLong

						val hypothesis = lFunc.computeHypothesis(x, weights)
						val w = hypothesis._2.mul(DoubleMatrix.ones(y.length).sub(hypothesis._2))
						val wre = DoubleMatrix.ones(y.length).divi(w)
						val z = hypothesis._1.add(diagonalMmul(wre, y.sub(hypothesis._2)))
						// instead of XtW = mmulDiagonal(x.transpose(), w), we directory transpose and mmul to save memory
						val XtW = tMmulDiagonal(x, w)
						XtWX = XtW.mmul(x)
						XtWz = XtW.mmul(z)

						//println(hypothesis.toString)
						//println(w.toString())
					}

				}
			}
		}

		if (nRows == 0) {
			XtWX = DoubleMatrix.zeros(weights.length, weights.length)
			XtWz = DoubleMatrix.zeros(weights.length, 1)
		}

		Iterator((XtWX, XtWz, nRows))
	}

	def doIRLS(data: RDD[(Matrix, Vector)], initialWeights: Array[Double], nFeatures: Int, eps: Double): (Vector, DoubleMatrix, Array[Double], Int, Long) = {
		// The best and safe value for initial weights is zeros, thus the logistic value is 0.5
		// The reason is the logistic function is more likely to be 0 or 1 which makes its derivative 0 all the way, :((((( 
		var weights = if (initialWeights == null || initialWeights.length != nFeatures) Vector(DoubleMatrix.zeros(nFeatures)) else Vector(initialWeights)

		val lossFunction = new LogisticRegression.LossFunction(data, ridgeLambda)

		var lastDev = Double.MaxValue
		var computedLoss = lossFunction.compute(weights)
		println(computedLoss.gradients.toString)
		var currentDev = 2 * computedLoss.loss
		val numSamples: Long = computedLoss.numSamples

		if (numSamples == 0)
			throw new RuntimeException("No data to run, there is no rows, may be it is due to the null filtering process.")

		var XtWXlambda: DoubleMatrix = null

		var iter = 0
		var deviances = Array(currentDev)

		//println("Iter " + iter + ": (lastDev: " + lastDev + ",currentDev: " + currentDev + "), eps: " + Math.abs((lastDev - currentDev) / currentDev))
		//println("Iter " + iter + ":" + weights.toString())

		while (math.abs((lastDev - currentDev) / (math.abs(currentDev) + 0.1)) >= eps && iter < numIters) {
			val ret = data.mapPartitions(handlePartition(lossFunction, weights))
				.reduce((x, y) ⇒ (x._1.addi(y._1), x._2.addi(y._2), x._3 + y._3))

			val lastXtWXlambda = XtWXlambda
			XtWXlambda = ret._1

			if (ridgeLambda != 0) {
				XtWXlambda = XtWXlambda.addi(DoubleMatrix.eye(nFeatures).muli(ridgeLambda))
			}
			val lastWeights = weights
			weights = Vector(Solve.solve(XtWXlambda, ret._2))

			lastDev = currentDev

			var computedLoss = lossFunction.compute(weights)
			currentDev = 2 * computedLoss.loss

			iter += 1
			//println("Iter " + iter + ": XtWXlambda " + XtWXlambda.toString())
			//println("Iter " + iter + ": XtWz " + ret._2.toString())
			//println("Iter " + iter + ": (lastDev: " + lastDev + ",currentDev: " + currentDev + "), eps: " + math.abs((lastDev - currentDev) / currentDev))
			//println("Iter " + iter + ":" + weights.toString())

			if (currentDev.isNaN()) {
				XtWXlambda = lastXtWXlambda
				weights = lastWeights
				iter -= 1
			}
			else {
				deviances :+= currentDev
			}
		}

		(weights, XtWXlambda, deviances, iter, numSamples)
	}

	def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): IRLSLogisticRegressionModel = {
		if (numIters == 0) // when client does not send numIters to this executor 
			numIters = 25
		if (eps == 0) // when client does not send eps to this executor 
			eps = 1e-8

		if (!nullModel) {
			val ret = doIRLS(dataPartition, initialWeights, numFeatures, eps)

			val invXtWX = Solve.solvePositive(ret._2, DoubleMatrix.eye(numFeatures))

			// standard errors
			val stderrs = org.jblas.MatrixFunctions.sqrt(invXtWX.diag())

			return new IRLSLogisticRegressionModel(ret._1, ret._3(ret._4), 0, ret._5, numFeatures - 1, ret._4, Vector(stderrs))
		}
		else {
			// This is when user want the null deviance only
			// Run IRLS again to calculate nullDeviance which is deviance of null model(only the intercept)
			val nullPartition = dataPartition.map { case (x, y) ⇒ (Matrix.ones(y.length), y) }.cache()
			//val p = nullPartition.collect()(0)
			//println(p._1.getRows())
			//println(p._2.getRows())
			val retNull = doIRLS(nullPartition, null, 1, eps)
			return new IRLSLogisticRegressionModel(retNull._1, retNull._3(retNull._4), retNull._3(retNull._4), retNull._5, 1, retNull._4, null)
		}

	}

	//post process, set column mapping to model
	def instrumentModel(model: IRLSLogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]): IRLSLogisticRegressionModel = {
		model.dummyColumnMapping = mapping
		model
	}
}

class IRLSLogisticRegressionModel(weights: Vector, val deviance: Double, val nullDeviance: Double, numSamples: Long, val numFeatures: Long, val numIters: Int, val stderrs: Vector) extends ALinearModel[Double](weights, numSamples) {
	override def predict(features: Vector): Double = ALossFunction.sigmoid(this.linearPredictor(features))
}

class IRLSDiscreteLogisticRegressionModel(weights: Vector, val deviances: Vector, numSamples: Long, val numIters: Int) extends ALinearModel[Int](weights, numSamples) {
	override def predict(features: Vector): Int = if (ALossFunction.sigmoid(this.linearPredictor(features)) < 0.5) 0 else 1
}

/**
 * Entry point for SparkThread executor to execute predictions
 */
class IRLSLogisticRegressionPredictor(val model: IRLSLogisticRegressionModel, val features: Array[Double]) extends APredictionExecutor[java.lang.Double] {
	def predict: java.lang.Double = model.predict(features).asInstanceOf[java.lang.Double]
}
