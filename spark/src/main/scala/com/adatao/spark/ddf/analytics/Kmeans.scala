package com.adatao.spark.ddf.analytics

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import com.adatao.ddf.{DDFManager, DDF}
import org.apache.spark.rdd.RDD
import com.adatao.ddf.analytics.MLSupporter.{ AAlgorithm, Model }
import com.adatao.ddf.analytics.ISupportML.{IModelParameters, IAlgorithm, IHyperParameters, IModel}
import java.util.{List => JList}
import com.adatao.ddf.content.Schema
import com.adatao.spark.ddf.SparkDDF

/**
 */
class Kmeans(params: KmeansParameters) extends AAlgorithm(classOf[Array[Double]], params) {

	def this() = this(new KmeansParameters())

	override def run(data: Object): KmeansModel = {
		val model = KMeans.train(data.asInstanceOf[RDD[Array[Double]]], params.numCentroids, params.maxIter, params.runs, params.initMode)
		new KmeansModel(params, model)
	}
}

class KmeansParameters(val numCentroids: Int = 10,
	val maxIter: Int = 100,
	val runs: Int = 10,
	val initMode: String = "random",
	val epsilon: Double = 1e-3) extends IModelParameters

class KmeansModel(predictionInputClass: Class[_], val params: KmeansParameters, featureColumnNames: JList[String],
                  val mllibKmeansModel: KMeansModel) extends Model(predictionInputClass, params, featureColumnNames) {

  override def predictImpl(data: Object, ddfName: String): DDF = {
    val rdd= data.asInstanceOf[RDD[Array[Double]]]
    val predRDD = rdd.map{
      point => mllibKmeansModel.predict(point)
    }
    val manager = DDFManager.get("spark")
    val schema = new Schema(String.format("%s_%s", ddfName, "kmeans_prediction"), "clusterID int")

    new SparkDDF(manager, predRDD, classOf[Double], manager.getNamespace, schema.getTableName, schema)
  }
}