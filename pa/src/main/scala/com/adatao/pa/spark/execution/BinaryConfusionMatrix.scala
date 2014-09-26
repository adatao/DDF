package com.adatao.pa.spark.execution

import com.adatao.spark.ddf.analytics._
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import io.ddf.DDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import org.apache.spark.mllib.classification.NaiveBayesModel
import io.spark.ddf.SparkDDF
import com.adatao.spark.ddf.ml.MLSupporter

/**
 * Compute the confusion matrix for a binary classification model, given a threshold.
 * The given model should be able to predict y such that 0 <= y <= 1.
 * @author aht
 */
class BinaryConfusionMatrix(dataContainerID: String, val modelID: String, val threshold: Double) extends AExecutor[BinaryConfusionMatrixResult] {

  override def runImpl(context: ExecutionContext): BinaryConfusionMatrixResult = {
    val ddfManager = context.sparkThread.getDDFManager()
    val model = ddfManager.getModel(modelID)
    val cm = model.getRawModel match {
      case linearModel: ALinearModel[Double] => {
        val ddf = ddfManager.getDDF(dataContainerID)
        ddf.getMLSupporter.getConfusionMatrix(model, threshold)
      }
      case otherwise => {
        val yTrueYPred = new YtrueYpred(dataContainerID, modelID).runImpl(context)
        val predictionDDF = ddfManager.getDDF(yTrueYPred.getDataContainerID)
        val predictionRDD = predictionDDF.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]])
        MLSupporter.binaryConfusionMatrix(predictionRDD, threshold)
      }
    }

    new BinaryConfusionMatrixResult(cm(0)(0), cm(1)(0), cm(0)(1), cm(1)(1))
  }
}

class BinaryConfusionMatrixResult(val truePos: Long, val falsePos: Long, val falseNeg: Long, val trueNeg: Long)
