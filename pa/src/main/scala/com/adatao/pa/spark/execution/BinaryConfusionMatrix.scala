package com.adatao.pa.spark.execution

import com.adatao.spark.ddf.analytics._
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import io.ddf.DDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

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
      case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL,
        s"Don't know how to get confusion matrix for ${model.getRawModel.getClass.toString}}", null)
    }

    new BinaryConfusionMatrixResult(cm(0)(0), cm(1)(0), cm(0)(1), cm(1)(1))
  }
}

class BinaryConfusionMatrixResult(val truePos: Long, val falsePos: Long, val falseNeg: Long, val trueNeg: Long)
