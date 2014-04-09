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
import java.util.List
import java.util.ArrayList

import com.adatao.ddf.DDF
import scala.collection.mutable.ArrayBuffer
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
		extends AModelTrainer[LogisticRegressionModel](dataContainerID, xCols, yCol) {
    
  override def train(dataContainerID: String, context: ExecutionContext): LogisticRegressionModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_")) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }
    // project the xCols, and yCol as a new DDF
    // this is costly
    val schema = ddf.getSchema()
    var columnList : java.util.List[java.lang.String] = new java.util.ArrayList[java.lang.String]
    for (col <- xCols) columnList.add(schema.getColumn(col).getName)
    columnList.add(schema.getColumn(yCol).getName)
    val projectDDF = ddf.Views.project(columnList)
    val logisticModel = projectDDF.ML.train("logisticRegressionWithSGD", numIters:java.lang.Integer, ridgeLambda: java.lang.Double)
    
    // converts DDF model to old PA model
    val rawModel = logisticModel.getRawModel.asInstanceOf[org.apache.spark.mllib.classification.LogisticRegressionModel]
    val paWeights: ArrayBuffer[Double] = ArrayBuffer[Double]()
    paWeights += rawModel.intercept
    for (w <- rawModel.weights) paWeights += w
    val weights = Vector.apply(paWeights.toArray)
    val trainingLoss: ArrayBuffer[Double] = ArrayBuffer[Double]()
    for (i <- 0 to numIters) trainingLoss += 0
    val paModel = new LogisticRegressionModel(weights, Vector.apply(trainingLoss.toArray), projectDDF.getNumRows())
    ddfManager.addModel(logisticModel)
    paModel.ddfModel = logisticModel
    return paModel
  }
  
	def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): LogisticRegressionModel = {
		null
	}
	
	//post process, set column mapping to model
	def instrumentModel(model: LogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) :LogisticRegressionModel = {
	  model.dummyColumnMapping = mapping
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
		def compute: Vector ⇒ ALossFunction = {
			(weights: Vector) ⇒ XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
		}
	}
}

/**
 * Entry point for SparkThread executor to execute predictions
 */
class LogisticRegressionPredictor(val model: LogisticRegressionModel, val features: Array[Double]) extends APredictionExecutor[java.lang.Double] {
	def predict: java.lang.Double = model.predict(features).asInstanceOf[java.lang.Double]
}
