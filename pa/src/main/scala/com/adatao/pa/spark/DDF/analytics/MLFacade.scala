package com.adatao.pa.spark.DDF.analytics

import com.adatao.ddf.ml.IModel
import com.adatao.pa.spark.execution.{LogisticRegressionIRLS, LinearRegressionNormalEquation, Kmeans}
import com.adatao.pa.spark.DDF.DDFManager._
import java.util.HashMap
import scala.math
import com.adatao.pa.spark.DDF.DDF

/**
 * author: daoduchuan
 */
class MLFacade(val ddf: DDF) {

  def Kmeans(xCols: Array[Int], numIterations: Int, K: Int, initializationMode: String= "random"): IModel = {
    val cmd = new Kmeans(this.ddf.name, xCols, numIterations, K, null, "random")
    client.execute[IModel](cmd).result
  }

  def Kmeans(xCols: Array[String], numIterations: Int, K: Int): IModel = {
    val xColsId = xCols.map{idx => this.ddf.getSchema.getColumnIndex(idx)}
    Kmeans(xColsId, numIterations, K)
  }

  def LinearRegressionNormalEquation(xCols: Array[Int], yCol: Int, ridgeLambda: Double = 0,
                                     mapReference: HashMap[String, String] = null): IModel = {
    val cmd = new LinearRegressionNormalEquation(this.ddf.name, xCols, yCol, ridgeLambda, mapReference)
    client.execute[IModel](cmd).result
  }

  def LinearRegressionNormalEquation(xCols: Array[String], yCol: String): IModel = {

    val xColsId= xCols.map{idx => this.ddf.getSchema.getColumnIndex(idx)}
    val yColID = this.ddf.getSchema.getColumnIndex(yCol)

    LinearRegressionNormalEquation(xColsId, yColID)
  }

  def LogisticRegression(xCols: Array[Int], yCol: Int, numIters: Int = 10, eps: Double = math.pow(10, -8), ridgeLambda: Double = 0,
                         initialWeights: Array[Double] = null, mapReference: HashMap[String, String] = null,
                         nullModel: Boolean = false)  = {
    val weights =  if(initialWeights == null) {
      Array.fill[Double](xCols.size + 1)(0)
    } else initialWeights

    val cmd  =new LogisticRegressionIRLS(this.ddf.name, xCols, yCol , numIters, eps, ridgeLambda, weights, mapReference, nullModel)
    client.execute[IModel](cmd).result
  }

  def LogisticRegression(xCols: Array[String], yCol: String, numIters: Int, eps: Double): IModel = {
    val xColsId= xCols.map{idx => this.ddf.getSchema.getColumnIndex(idx)}
    val yColID = this.ddf.getSchema.getColumnIndex(yCol)
    LogisticRegression(xColsId, yColID, numIters, eps)
  }
}
