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
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import com.adatao.spark.RDDImplicits._
import org.apache.spark.rdd.RDD
import java.util.HashMap
import java.util.ArrayList

import com.adatao.ddf.DDF

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

    override def train(dataContainerID: String, context: ExecutionContext): LinearRegressionModel = {
      val ddfManager = context.sparkThread.getDDFManager();
      val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_")) match {
        case x: DDF => x
        case _ => throw new IllegalArgumentException("Only accept DDF")
      }
      // project the xCols, and yCol as a new DDF
      // this is costly
      val schema = ddf.getSchema()
      var columnList : java.util.List[java.lang.String] = new java.util.ArrayList[java.lang.String]
      for (col <- xCols) columnList.add(schema.getColumn(col).getName)
      columnList.add(schema.getColumn(yCol).getName)
      val projectDDF = ddf.Views.project(columnList)
      // val (weights, trainingLosses, numSamples) = Regression.train(lossFunction, numIters, learningRate, initialWeights, numFeatures)
      // new LinearRegressionModel(weights, trainingLosses, numSamples)
      val model = projectDDF.ML.train("linearRegressionWithGD", xCols, yCol: java.lang.Integer, numIters:java.lang.Integer, learningRate: java.lang.Double, ridgeLambda: java.lang.Double, initialWeights)
      
      // converts DDF model to old PA model
      val rawModel = model.getRawModel.asInstanceOf[com.adatao.ML.LinearRegressionModel]
      val paModel = new LinearRegressionModel(rawModel.weights, rawModel.trainingLosses, projectDDF.getNumRows())
      ddfManager.addModel(model)
      paModel.ddfModel = model
      return paModel
    }
  
    def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): LinearRegressionModel = {
      null
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
        def compute: Vector => ALossFunction = {
            (weights: Vector) => XYData.map { case (x, y) => this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
        }
    }
}

/**
 * Entry point for SparkThread executor to execute predictions
 */
class LinearRegressionPredictor(val model: LinearRegressionModel, var features: Array[Double]) extends APredictionExecutor[java.lang.Double] {
    def predict: java.lang.Double = model.predict(features).asInstanceOf[java.lang.Double]
}
