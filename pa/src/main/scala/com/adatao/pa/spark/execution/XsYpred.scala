package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.spark.ddf.analytics.Utils
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * author: daoduchuan
 */
class XsYpred(dataContainerID: String, val modelID: String) extends AExecutor[DataFrameResult]{

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val ddfManager = ctx.sparkThread.getDDFManager
    val ddfID = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfID)

    val model = ddfManager.getModel(modelID)
    if(model == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "null model", null)
    }
    val featureColumns = model.getTrainedColumns
    val projectedDDF = ddf.VIEWS.project(featureColumns: _*)

    val predictionDDF = projectedDDF.getMLSupporter.applyModel(model, false, true)
    ddfManager.addDDF(predictionDDF)

    new DataFrameResult(predictionDDF)
  }
}
