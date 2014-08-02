package com.adatao.pa.spark.execution

import com.adatao.spark.ddf.analytics.Utils

/**
 * author: daoduchuan
 */
class applyModel(dataContainerID: String, var modelID: String) extends AExecutor[applyModelResult] {
  override def runImpl(ctx: ExecutionContext): applyModelResult = {

    val ddfManager = ctx.sparkThread.getDDFManager
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfId)
    val model = ddfManager.getModel(modelID);
    val newDDF = ddf.ML.applyModel(model, true, true);
    return new applyModelResult(newDDF.getName());
  }
}

class applyModelResult(val dataContainerID: String)
