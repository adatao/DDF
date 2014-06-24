package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.{ SharkUtils, DataManager }
import com.adatao.ML.{ TModel, ALinearModel, TPredictiveModel }
import com.adatao.ddf.types.Vector
import com.adatao.ML.spark.RddUtils
import org.apache.spark.api.java.JavaRDD
import shark.api.JavaSharkContext
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.ML.Utils
import com.adatao.ddf.exception.DDFException

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
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf: DDF = ddfManager.getDDF(ddfId)

    //apply model on dataContainerID
    val model: IModel = ddfManager.getModel(modelID)
    val projectedDDF = ddf.Views.project(model.getTrainedColumns: _*)

    val predictionDDF = projectedDDF.getMLSupporter().applyModel(model, true, false)

    val predDDFID = if(predictionDDF == null) {
        throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error predicting, prediction DDF is null.", null)
    } else {
        ddfManager.addDDF(predictionDDF)
    }

    val metaInfo = Array(new MetaInfo("ytrue", "java.lang.Double"), new MetaInfo("yPredict", "java.lang.Double"))
    val uid = predictionDDF.getName().replace("_", "-").replace("SparkDDF-spark-", "").replace("-com.adatao.ML.LogisticRegressionModel-YTrueYPredict", "")

    LOG.info(">>>>>>dataContainerID = " + dataContainerID + "\t predictionDDF id =" + uid + "\taddId=" + predDDFID)

    new YtrueYpredResult(uid, metaInfo)
  }
}

class YtrueYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
