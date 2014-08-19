package com.adatao.pa.spark.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer

import io.ddf.DDF
import io.spark.ddf.content.RepresentationHandler
import io.ddf.ml.IModel
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.pa.spark.types.{ExecutionException, SuccessfulResult, FailedResult, ExecutionResult}

class Kmeans(
  dataContainerID: String,
  xCols: Array[Int],
  val numIterations: Int,
  val K: Int,
  initialCentroids: java.util.List[Array[Double]] = null,
  val initializationMode: String,
  var initializationSteps: Int = 5,
  var epsilon: Double = 1e-4)
  extends AExecutor[IModel](true) {
  
  override def runImpl(ctx: ExecutionContext) = train(dataContainerID, ctx)
  
  def train(dataContainerID: String, context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddf = ddfManager.getDDF(dataContainerID) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }
    // project the xCols, and yCol as a new DDF
    // this is costly

    val trainedColumns = xCols.map(idx => ddf.getColumnName(idx))
    val projectedDDF = ddf.VIEWS.project(trainedColumns: _*)

//    projectedDDF.ML.train("kmeans", K: java.lang.Integer, numIterations: java.lang.Integer)
    projectedDDF.ML.KMeans(K, numIterations, 1)
  }
}

object Kmeans {
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"
}





