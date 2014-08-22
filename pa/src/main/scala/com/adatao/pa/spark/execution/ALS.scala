package com.adatao.pa.spark.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer

import io.ddf.DDF
import io.spark.ddf.content.RepresentationHandler
import io.ddf.ml.IModel
import com.adatao.pa.spark.types.{ ExecutionException, SuccessfulResult, FailedResult, ExecutionResult }

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext._
class ALS(
  dataContainerID: String,
  xCols: Array[Int],
  val rank: Int,
  val numIterations: Int,
  val lamda: Double)
  extends AExecutor[IModel](true) {
  override def runImpl(ctx: ExecutionContext) = train(dataContainerID, ctx)

  def train(dataContainerID: String, context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddf = ddfManager.getDDF(dataContainerID) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }

    val trainedColumns = xCols.map(idx => ddf.getColumnName(idx))
    val trainedData = ddf.VIEWS.project(trainedColumns: _*)

    trainedData.ML.train("collaborateFiltering", rank: java.lang.Integer, numIterations: java.lang.Integer, lamda: java.lang.Double)
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {
    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}

