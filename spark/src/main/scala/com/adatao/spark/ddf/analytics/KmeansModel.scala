package com.adatao.spark.ddf.analytics

import com.adatao.ddf.analytics.MLSupporter.Model
import org.apache.spark.mllib.clustering.{KMeansModel => MLlibKmeansModel}
import com.adatao.ddf.{DDFManager, DDF}
import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.Schema
import com.adatao.spark.ddf.SparkDDF
import java.util.{List => JList}
/**
 * author: daoduchuan
 */
class KmeansModel(mllibKmeansModel: MLlibKmeansModel, featureColumnNames: JList[String]) extends
  Model(featureColumnNames, classOf[Array[Double]]) {

  def this(mllibKmeansModel: MLlibKmeansModel) = this(mllibKmeansModel, null)

  def getMllibKmeansModel() = this.mllibKmeansModel

  override def isSupervisedAlgorithmModel(): Boolean = false

  override def predict(data :Object, ddf: DDF): DDF = {
    val rdd= data.asInstanceOf[RDD[Array[Double]]]
    val predRDD = rdd.map(point => mllibKmeansModel.predict(point))

    val schema = new Schema(String.format("%s_%s", ddf.getName, "kmeans_prediction"), "clusterID int")

    new SparkDDF(ddf.getManager, predRDD, classOf[Int], ddf.getManager.getNamespace, schema.getTableName, schema)
  }

  override def predictImpl(point: Array[Double]): Double = {
    mllibKmeansModel.predict(point).toDouble
  }
}
