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
import com.adatao.ML.ALossFunction
import com.adatao.ML.LinearRegressionModel
import com.adatao.ML.Utils
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.spark.RDDImplicits._
import org.apache.spark.rdd.RDD
import java.util.HashMap

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
		extends AModelTrainer[LinearRegressionModel](dataContainerID, xCols, yCol) {

	def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): LinearRegressionModel = {

		//depend on length of weights
		val weights = if (initialWeights == null || initialWeights.length != numFeatures)  Utils.randWeights(numFeatures) else Vector(initialWeights)
		var model = ML.LinearRegression.train(
			new LinearRegression.LossFunction(dataPartition, ridgeLambda), numIters, learningRate, weights, numFeatures
		)

		model
	}
	//post process, set column mapping to model
	def instrumentModel(model: LinearRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) :LinearRegressionModel = {
	  model.dummyColumnMapping = mapping
	  model
	}
}

object LinearRegression {
	/**
	 * As a client with our own data representation [[RDD(Matrix, Vector]], we need to supply our own LossFunction that
	 * knows how to handle that data.
	 *
	 * NB: We separate this class into a static (companion) object to avoid having Spark serialize too many unnecessary
	 * objects, if we were to place this class within [[class LinearRegression]].
	 */
	class LossFunction(@transient XYData: RDD[(Matrix, Vector)], ridgeLambda: Double) extends ML.ALinearGradientLossFunction(XYData, ridgeLambda) {
		def compute: Vector ⇒ ALossFunction = {
			(weights: Vector) ⇒ XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
		}
	}
}

/**
 * Entry point for SparkThread executor to execute predictions
 */
class LinearRegressionPredictor(val model: LinearRegressionModel, var features: Array[Double]) extends APredictionExecutor[java.lang.Double] {
	def predict: java.lang.Double = model.predict(features).asInstanceOf[java.lang.Double]
}
