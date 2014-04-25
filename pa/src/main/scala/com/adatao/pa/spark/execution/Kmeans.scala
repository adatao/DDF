package com.adatao.pa.spark.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD

import com.adatao.ML.KmeansModel
import com.adatao.ddf.DDF
import com.adatao.spark.ddf.content.RepresentationHandler

class Kmeans(
  dataContainerID: String,
  xCols: Array[Int],
  val numIterations: Int,
  val K: Int,
  initialCentroids: java.util.List[Array[Double]] = null,
  val initializationMode: String,
  var initializationSteps: Int = 5,
  var epsilon: Double = 1e-4)
    extends AUnsupervisedTrainer[KmeansModel](dataContainerID, xCols) {
  override def train(dataContainerID: String, context: ExecutionContext): KmeansModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_")) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }
    // project the xCols, and yCol as a new DDF
    // this is costly
    val schema = ddf.getSchema()
    var columnList: java.util.List[java.lang.String] = new java.util.ArrayList[java.lang.String]
    for (col ← xCols) columnList.add(schema.getColumn(col).getName)
    val projectedDDF = ddf.Views.project(columnList)
    val kmeansModel = projectedDDF.ML.train("kmeans", K: java.lang.Integer, numIterations: java.lang.Integer)

    // converts DDF model to old PA model
    val rawModel = kmeansModel.getRawModel.asInstanceOf[org.apache.spark.mllib.clustering.KMeansModel]

    val wcss = rawModel.computeCost(projectedDDF.getRepresentationHandler().get(RepresentationHandler.RDD_ARRAY_DOUBLE).asInstanceOf[RDD[Array[Double]]])
    val totalWithins: ArrayBuffer[Double] = ArrayBuffer[Double]()
    val pointsPerCluster: ArrayBuffer[Int] = ArrayBuffer[Int]()
    for (i ← 1 to numIterations) {
      totalWithins += 0
    }
    for (i ← 1 to K) {
      pointsPerCluster += 0
    }
    return new KmeansModel(wcss, pointsPerCluster.toArray, rawModel.clusterCenters.toList, projectedDDF.getNumRows())
  }

  def train(dataPartition: RDD[Array[Double]], context: ExecutionContext): KmeansModel = {
    null
  }
}

object Kmeans {
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"
}




