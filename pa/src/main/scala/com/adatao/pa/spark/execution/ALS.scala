package com.adatao.pa.spark.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer

import io.ddf.DDF
import io.spark.ddf.content.RepresentationHandler
import io.ddf.ml.IModel
import io.ddf.ml.Model
import com.adatao.pa.spark.types.{ ExecutionException, SuccessfulResult, FailedResult, ExecutionResult }

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext._
import org.jblas.{ DoubleMatrix, SimpleBlas, Solve }
import com.adatao.ML.spark.ALSUtils._

class ALS(
  dataContainerID: String,
  xCols: Array[Int],
  val numFeatures: Int,
  val lamda: Double,
  val numIterations: Int,
  var implicitFeedback: Boolean = false,
  var alpha: Double = 1.0,
  var seed: Long = System.nanoTime())
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

    val imodel = trainedData.ML.train("collaborativeFiltering", numFeatures: java.lang.Integer, numIterations: java.lang.Integer, lamda: java.lang.Double)
    val matrixFactorizationModel = imodel.getRawModel.asInstanceOf[MatrixFactorizationModel]
    var numUsers = matrixFactorizationModel.userFeatures.count.toInt
    var numProducts = matrixFactorizationModel.productFeatures.count.toInt

    print(">>>>>>>>>>>IN ALS train " + numUsers + "and " + numProducts);
    val alsModel = ALS.computeALSModel(matrixFactorizationModel, numUsers, numProducts, numFeatures)
    
    val model = new Model(alsModel)
    ddfManager.addModel(model);
    return model;
  }
}

object ALS {

  def computeALSModel(model: MatrixFactorizationModel, users: Int, products: Int, features: Int): ALSModel = {
    val userFeatures = getUserFeatureMatrix(model, users, features);
    val productFeatures = getProductFeatureMatrix(model, products, features);
    new ALSModel(features, userFeatures, productFeatures)
  }
  
/*  def toDoubleMatrix(componentMatrix: RDD[(Int, Array[Double])], m: Int, n: Int): DoubleMatrix = {
    val prediction = new DoubleMatrix(m, n)
    val list = componentMatrix.collect();
    
    for (i <- 0 to n) {
      for (predicted <- list) {
        prediction.put(predicted._1.asInstanceOf[Integer], i, predicted._2(i));
      }
    }
    prediction
  }

  def getUserFeatureMatrix(model: MatrixFactorizationModel, users: Int, features: Int): DoubleMatrix = {
    toDoubleMatrix(model.userFeatures, users, features);
  }
  
  def getProductFeatureMatrix(model: MatrixFactorizationModel, products: Int, features: Int): DoubleMatrix = {
    toDoubleMatrix(model.productFeatures, products, features);
  }*/

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitFeedback: Boolean) = {
    def mapPredictedRating(r: Double) = if (implicitFeedback) math.max(math.min(r, 1.0), 0.0) else r
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
  
}
