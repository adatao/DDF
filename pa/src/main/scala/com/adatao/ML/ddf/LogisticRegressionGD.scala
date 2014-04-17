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

package com.adatao.ML.ddf

import java.lang.String
import com.adatao.ML
import com.adatao.ML.Utils
import com.adatao.ML.TModel
import com.adatao.ddf.types.TupleMatrixVector
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
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
object LogisticRegressionGD {
    
	def train(dataPartition: RDD[TupleMatrixVector],
	numFeatures:java.lang.Integer,
    xCols: Array[Int],
    yCol: Int,
    numIters: Int,
    learningRate: Double,
    ridgeLambda: Double,
    initialWeights: Array[Double]): LogisticRegressionModel = {
//    val numFeatures = xCols.length + 1
//    println(">>>>> lm-numFeatures:" + numFeatures + "\tdataPartition.take(1).rows=" + dataPartition.take(1).rows)
    //depend on length of weights
    val weights = if (initialWeights == null || initialWeights.length != numFeatures)  Utils.randWeights(numFeatures) else Vector(initialWeights)
    
    var model = ML.LogisticRegression.train(
      new LogisticRegressionGD.LossFunction(dataPartition.map {row => (row._1, row._2)}, ridgeLambda), numIters, learningRate, weights, numFeatures
    )
    model
  }
	
	//post process, set column mapping to model
	def instrumentModel(model: LogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) :LogisticRegressionModel = {
	  model.dummyColumnMapping = mapping
	  model
	}
	
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
