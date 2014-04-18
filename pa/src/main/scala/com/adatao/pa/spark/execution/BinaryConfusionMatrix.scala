package com.adatao.pa.spark.execution

import com.adatao.ML.{ TModel, ALinearModel, TPredictiveModel }
import com.adatao.ddf.types.Vector
import com.adatao.ML.spark.{ Metrics, RddUtils }
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType

import com.adatao.ddf.DDF

/**
 * Compute the confusion matrix for a binary classification model, given a threshold.
 * The given model should be able to predict y such that 0 <= y <= 1.
 * @author aht
 */
class BinaryConfusionMatrix(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int, val threshold: Double) extends AExecutor[BinaryConfusionMatrixResult] {

  override def runImpl(context: ExecutionContext): BinaryConfusionMatrixResult = {
    val ddfManager = context.sparkThread.getDDFManager()
    val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_")) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
    // val ddfModelID = context.sparkThread.getDataManager.getObject(modelID).asInstanceOf[TModel].ddfModelID
    val ddfModel = ddfManager.getModel(modelID)
    val cm = ddf.ML.getConfusionMatrix(ddfModel, threshold)
    new BinaryConfusionMatrixResult(cm(0)(0), cm(1)(0), cm(0)(1), cm(1)(1))
  }
}

class BinaryConfusionMatrixResult(val truePos: Long, val falsePos: Long, val falseNeg: Long, val trueNeg: Long)
