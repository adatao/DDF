package com.adatao.spark.ddf.analytics

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import com.adatao.ddf.DDF
import org.apache.spark.rdd.RDD
import com.adatao.ddf.analytics.MLSupporter.{ AAlgorithm, Model }
import com.adatao.ddf.analytics.ISupportML.{ IAlgorithm, IHyperParameters, IModel }

/**
 */
class Kmeans(params: KmeansParameters) extends AAlgorithm(classOf[Array[Double]], params) {

	def this() = this(new KmeansParameters())

	override def run(data: Object): KmeansModel = {
		val model = KMeans.train(data.asInstanceOf[RDD[Array[Double]]], params.numCentroids, params.maxIter, params.runs, params.initMode)
		new KmeansModel(data, params, model)
	}
}

class KmeansParameters(val numCentroids: Int = 10,
	val maxIter: Int = 100,
	val runs: Int = 10,
	val initMode: String = "random",
	val epsilon: Double = 1e-3) extends IHyperParameters

class KmeansModel(val data: Object, val params: KmeansParameters, val mllibKmeansModel: KMeansModel) extends Model {
}