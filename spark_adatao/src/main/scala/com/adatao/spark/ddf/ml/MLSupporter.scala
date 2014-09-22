package com.adatao.spark.ddf.ml

import io.spark.ddf.ml.{MLSupporter => IOMLSupporter}
import io.ddf.DDF
import io.ddf.ml.{ISupportML, IModel}
import com.adatao.spark.ddf.analytics.{ALinearModel, AContinuousIterativeLinearModel}
import com.adatao.spark.ddf.etl.TransformationHandler._
import io.ddf.types.TupleMatrixVector
import org.apache.spark.rdd.RDD
import io.ddf.etl.IHandleTransformations
import io.ddf.exception.DDFException

/**
 */
class MLSupporter(mDDF: DDF) extends IOMLSupporter(mDDF) {

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = {
    val trainedColumns = model.getTrainedColumns
    val xCols = trainedColumns.take(trainedColumns.size - 1)
    val yCol = trainedColumns(trainedColumns.size - 1)
    val iThreshold = threshold
    model.getRawModel match {
      case linearModel: ALinearModel[Double] => {
        mLog.info(">>> ALinearModel, running dummyCoding transformation")
        val dummyCodingDDF = this.mDDF.getTransformationHandler.dummyCoding(xCols, yCol)
        val rddMatrixVector = dummyCodingDDF.getRDD(classOf[TupleMatrixVector])
        val yTrueYPred = linearModel.yTrueYPred(rddMatrixVector)
        val result = MLSupporter.binaryConfusionMatrix(yTrueYPred, iThreshold)
        Array(Array(result(3), result(2)), Array(result(1), result(0)))
      }
    }
  }
}

object MLSupporter {

  implicit def MLSupporterConversion(tHandler: ISupportML): MLSupporter = {
    tHandler match {
      case trHandler: MLSupporter => trHandler
      case x => throw new DDFException(String.format("Could not convert %s into com.adatao.spark.ddf.etl.TransformationHandler", x.getClass.toString))
    }
  }

  def toByte(b: Boolean): Byte = if (b) 1 else 0

  def binaryConfusionMatrix(yTrueYpred: RDD[Array[Double]], threshold: Double): Array[Long] = {
    yTrueYpred.map {
      case Array(ytrue, ypred) ⇒
        // ground truth == positive?
        val isPos = toByte(ytrue > threshold)

        // predicted == positive?
        val predPos = toByte(ypred > threshold)

        val result = Array[Long](0L, 0L, 0L, 0L) // TN, FP, FN, TP, the logic is defined by next line
        result(isPos << 1 | predPos) = 1
        result
    }.reduce((a, b) ⇒ Array(a(0) + b(0), a(1) + b(1), a(2) + b(2), a(3) + b(3)))
  }
}
