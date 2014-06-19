package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.ML.Utils

/**
 * author: daoduchuan
 */
class XsYpred(dataContainerID: String, val modelID: String, val xCols: Array[Int]) extends AExecutor[DataFrameResult]{

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val ddfManager = ctx.sparkThread.getDDFManager
    val ddfID = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfID)

    val model = ddfManager.getModel(modelID)
    val projectedDDF = ddf.Views.project(model.getTrainedColumns: _*)

    val predictionDDF = projectedDDF.getMLSupporter.applyModel(model, false, true)
    new DataFrameResult(predictionDDF)
  }
}
