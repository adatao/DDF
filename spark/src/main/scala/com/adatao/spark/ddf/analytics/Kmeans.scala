package com.adatao.spark.ddf.analytics

import com.adatao.ddf.analytics.{IAlgorithmOutputModel, IParameters, AAlgorithm, IAlgorithm}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import com.adatao.ddf.DDF
import org.apache.spark.rdd.RDD

/**
 * author: daoduchuan
 */
class Kmeans(params: KmeansParameters) extends AAlgorithm(classOf[Array[Double]], params){

  def this()= this(new KmeansParameters())

  override def preprocess(ddf: DDF) = {

    ddf.getRepresentationHandler.getRepresentation(this.getElementType)
  }

  override def run(ddf: DDF): KmeansModel = {

    val data= ddf.getRepresentationHandler.get(this.getElementType).asInstanceOf[RDD[Array[Double]]]

    val model= KMeans.train(data, params.numCentroids, params.maxIter, params.runs, params.initMode)

    new KmeansModel(ddf, params, model)
  }
}

class KmeansParameters(val numCentroids: Int = 10,
                       val maxIter: Int = 100,
                       val runs: Int = 10,
                       val initMode: String ="random",
                       val epsilon: Double = 1e-3) extends IParameters

class KmeansModel(val data: DDF, val params: KmeansParameters, val mllibKmeansModel: KMeansModel) extends IAlgorithmOutputModel {

  override def persist(): Unit = {

    //IMPLEMENTATION HERE
  }
}