package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.{ SharkUtils, DataManager }
import com.adatao.spark.ddf.analytics.{ TModel, TPredictiveModel }
import com.adatao.spark.ddf.analytics.ALinearModel
import io.ddf.types.Vector
import com.adatao.spark.ddf.analytics._
import org.apache.spark.api.java.JavaRDD
import shark.api.JavaSharkContext
import com.adatao.spark.ddf.analytics._
import io.ddf.DDF
import io.ddf.ml.IModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.spark.ddf.analytics.Utils
import io.ddf.exception.DDFException

/**
 * Return predictions pair (ytrue, ypred) RDD in a DataFrame,
 * where ytrue and ypred are doubles.
 *
 * Works with LinearRegressionModel and LogisticRegressionModel.
 *
 */ 
class YtrueYpred(dataContainerID: String, val modelID: String) extends AExecutor[YtrueYpredResult] {
  override def runImpl(ctx: ExecutionContext): YtrueYpredResult = {
    val ddfManager = ctx.sparkThread.getDDFManager()
    val ddf: DDF = ddfManager.getDDF(dataContainerID)

    //apply model on dataContainerID
    val model: IModel = ddfManager.getModel(modelID)
    val projectedDDF = ddf.Views.project(model.getTrainedColumns: _*)

    val predictionDDF = projectedDDF.getMLSupporter().applyModel(model, true, false)

    val predDDFID = if(predictionDDF == null) {
        throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error predicting, prediction DDF is null.", null)
    } else {
        val predID = ddfManager.addDDF(predictionDDF)
        LOG.info(">>>>> predDDFID  = " + predID)
    }

    val metaInfo = Array(new MetaInfo("ytrue", "java.lang.Double"), new MetaInfo("yPredict", "java.lang.Double"))
    val uid = predictionDDF.getName()

    LOG.info(">>>>>>dataContainerID = " + dataContainerID + "\t predictionDDF id =" + uid + "\taddId=" + predDDFID)

    new YtrueYpredResult(uid, metaInfo)
  }
}

class YtrueYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
