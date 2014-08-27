package com.adatao.pa.spark.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer

import io.ddf.DDF
import io.spark.ddf.content.RepresentationHandler
import io.spark.ddf.content.RepresentationHandler._
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
  var dataContainerID: String,
  var xCols: Array[Int],
  var numFeatures: Int,
  var lambda: Double,
  var numIterations: Int,
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

    val imodel = trainedData.ML.train("collaborativeFiltering", numFeatures: java.lang.Integer, numIterations: java.lang.Integer, lambda: java.lang.Double)
    val matrixFactorizationModel = imodel.getRawModel.asInstanceOf[MatrixFactorizationModel]

    //TODO: Assume all users and products are including in rating file, otherwise consider having numUsers and numProducts as input params
    var numUsers = matrixFactorizationModel.userFeatures.count.toInt
    var numProducts = matrixFactorizationModel.productFeatures.count.toInt

    print(">>>>>>>>>>>IN ALS train " + numUsers + "and " + numProducts);

    val rmse = ALS.computeRmse(matrixFactorizationModel, trainedData.getRepresentationHandler().get(classOf[RDD[_]], classOf[Rating]).asInstanceOf[RDD[Rating]], false)
    print(">>>>>>>>>>>IN ALS train RMES =" + rmse);
    val alsModel = ALS.computeALSModel(matrixFactorizationModel, numUsers, numProducts, numFeatures, rmse)

    val model = new Model(alsModel)
    ddfManager.addModel(model);
    return model;
  }

  def this() = this("dataContainerID", Array(0,1,2), 10, 0.01, 10)
  def setDataContainerID(dataContainerID: String): ALS = {
    this.dataContainerID = dataContainerID
    this
  }
  
  def setTrainColumns(xCols: Array[Int]): ALS = {
    this.xCols = xCols
    this
  }
  /** Set the rank of the feature matrices computed (number of features). Default: 10. */
  def setNumFeatures(rank: Int): ALS = {
    this.numFeatures = rank
    this
  }

  /** Set the number of iterations to run. Default: 10. */
  def setNumIterations(iterations: Int): ALS = {
    this.numIterations = iterations
    this
  }

  /** Set the regularization parameter, lambda. Default: 0.01. */
  def setLambda(lambda: Double): ALS = {
    this.lambda = lambda
    this
  }

  def setImplicitFeedback(implicitPrefs: Boolean): ALS = {
    this.implicitFeedback = implicitPrefs
    this
  }

  def setAlpha(alpha: Double): ALS = {
    this.alpha = alpha
    this
  }

  /** Sets a random seed to have deterministic results. */
  def setSeed(seed: Long): ALS = {
    this.seed = seed
    this
  }
}

object ALS {

  def computeALSModel(model: MatrixFactorizationModel, users: Int, products: Int, features: Int, rmse: Double): ALSModel = {
    val userFeatures = getUserFeatureMatrix(model, users, features);
    val productFeatures = getProductFeatureMatrix(model, products, features);
    new ALSModel(features, userFeatures, productFeatures, rmse)
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
