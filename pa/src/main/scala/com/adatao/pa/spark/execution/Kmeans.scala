package com.adatao.pa.spark.execution

import com.adatao.ML
import com.adatao.ML.KmeansModel
import com.adatao.ML.types.{Matrix, Vector}
import com.adatao.pa.spark.DataManager.{DataFrame, DataContainer, SharkDataFrame}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import com.adatao.ML.Kmeans.{ParsePoint, SharkParsePoint, DataPoint}
import java.util.List
import java.util.ArrayList

import com.adatao.ddf.DDF

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
	override def train(dataContainerID: String, context: ExecutionContext): KmeansModel = {
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
    val projectDDF = ddf.Views.project(columnList)
    val kmeansModel = projectDDF.ML.train("kmeans", K: java.lang.Integer, numIterations: java.lang.Integer)
    
    // converts DDF model to old PA model
    val rawModel = kmeansModel.getRawModel.asInstanceOf[org.apache.spark.mllib.clustering.KMeansModel]
    
    val totalWithins: ArrayBuffer[Double] = ArrayBuffer[Double]()
    val pointsPerCluster: ArrayBuffer[Int] = ArrayBuffer[Int]()
    for (i <- 1 to numIterations) {
      totalWithins += 0
    }
    for (i <- 1 to K) {
      pointsPerCluster += 0
    }
    return new KmeansModel(totalWithins.toArray, pointsPerCluster.toArray, rawModel.clusterCenters.toList, projectDDF.getNumRows())
  }
	
	def train(dataPartition: RDD[Array[Double]], context: ExecutionContext): KmeansModel = {
		null
	}
}

object Kmeans{
	val RANDOM = "random"
	val KPLUSPLUS = "k++"
}


