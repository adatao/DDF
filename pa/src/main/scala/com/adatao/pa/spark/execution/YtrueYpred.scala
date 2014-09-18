package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.DataManager
import com.adatao.spark.ddf.analytics.{ TModel, TPredictiveModel }
import com.adatao.spark.ddf.analytics.ALinearModel
import io.ddf.types.{TupleMatrixVector, Vector}
import com.adatao.spark.ddf.analytics._
import org.apache.spark.api.java.JavaRDD
import com.adatao.spark.ddf.analytics._
import io.ddf.DDF
import io.ddf.ml.IModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.spark.ddf.analytics.Utils
import io.ddf.exception.DDFException
import com.adatao.pa.spark.Utils._
import com.adatao.spark.ddf.etl.TransformationHandler
import io.ddf.content.Schema
import io.spark.ddf.SparkDDF
import io.spark.ddf.content.RepresentationHandler

/**
 * Return predictions pair (ytrue, ypred) RDD in a DataFrame,
 * where ytrue and ypred are doubles.
 *
 * Works with LinearRegressionModel and LogisticRegressionModel.
 *
 */ 
class YtrueYpred(dataContainerID: String, val modelID: String) extends AExecutor[DataFrameResult] {
  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val ddfManager = ctx.sparkThread.getDDFManager()
    val ddf: DDF = ddfManager.getDDF(dataContainerID)

    //apply model on dataContainerID
    val model: IModel = ddfManager.getModel(modelID)
    val trainedColumns = model.getTrainedColumns
    val (xs, y) = trainedColumns.splitAt(trainedColumns.size - 1)
//    val projectedDDF = ddf.VIEWS.project(model.getTrainedColumns: _*)
//
//    val predictionDDF = projectedDDF.getMLSupporter().applyModel(model, true, false)
    val predictionDDF = model.getRawModel match {
      case linearModel: ALinearModel[_] => {
        LOG.info(">>> ALinearModel, running dummyCoding transformation")
        val dummyCodingDDF = ddf.getTransformationHandler.asInstanceOf[TransformationHandler].dummyCoding(xs, y(0))
        val rddMatrixVector = dummyCodingDDF.getRDD(classOf[TupleMatrixVector])
        val ytrueYpredRDD = linearModel.yTrueYPred(rddMatrixVector)
        val schema = new Schema(null, "ytrue double, yPredict double")
        new SparkDDF(ddf.getManager, ytrueYpredRDD, classOf[Array[Double]], ddf.getNamespace, null, schema)
      }
      case _ => {
        val projectedDDF = ddf.VIEWS.project(model.getTrainedColumns: _*)
        projectedDDF.getMLSupporter().applyModel(model, true, false)
      }
    }

    if(predictionDDF == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error predicting, prediction DDF is null.", null)
    } else {
      val predID = ddfManager.addDDF(predictionDDF)
      LOG.info(">>>>> prediction DDF = " + predID)
    }
    new DataFrameResult(predictionDDF)
  }
}

