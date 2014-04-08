package com.adatao.pa.spark.execution

import com.adatao.ML
import com.adatao.ML.KmeansModel
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import com.adatao.pa.spark.DataManager.{DataFrame, DataContainer, SharkDataFrame}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import com.adatao.ML.Kmeans.{ParsePoint, SharkParsePoint, DataPoint}

/**
 * Created with IntelliJ IDEA.
 * Author: daoduchuan
 * Please note there're too places in the current implementation handling column type
 * and throwing exception:
 * 	1. method getDataPartition in AUnsupervisedTrainer
 * 	2. ML.Kmeans.ParsePoint and ML.Kmeans.SharkParsePoint
 *
 *
 */
class Kmeans(
		dataContainerID: String,
		xCols: Array[Int],
		val numIterations: Int,
		val K: Int,
		initialCentroids: java.util.List[Array[Double]] = null,
		val initializationMode: String = "random")
		extends AUnsupervisedTrainer[KmeansModel](dataContainerID, xCols){
	def train(dataPartition: RDD[Array[Double]], context: ExecutionContext): KmeansModel = {
		type PointType = Tuple3[Array[Double], Int, Double]

		val centroids: Array[PointType] = Option(initialCentroids) match {
			case None 	=> initializationMode match {
					case Kmeans.RANDOM => {
						LOG.info("Running using random initialization mode")
						dataPartition.takeSample(false, K, 42).map(point => (point, 1, 0.0))
					}
					//case Kmeans.KPLUSPLUS =>
					case x             => throw new Exception("Don't support this initialization mode" + x)
				}
			case Some(x) 		=> {
				LOG.info("Running using centroids provided by user")
				x.map(point => (point, 1, 0.0)).toArray
			}
		}

		ML.Kmeans.train(dataPartition, numIterations, centroids)
	}
}

object Kmeans{
	val RANDOM = "random"
	val KPLUSPLUS = "k++"
}


