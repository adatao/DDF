package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.basic.ddf.BasicDDF
import com.adatao.pa.spark.Utils
import com.adatao.pa.spark.DataManager.MetaInfo

/**
  */


class Serialize2DDF(modelID: String) extends AExecutor[DataFrameResult] {

  protected override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val model = manager.getModel(modelID)
    val newDDF = new BasicDDF(model)

    val name = manager.addDDF(newDDF)
    LOG.info(">>>>> put ddf to manager: " + name)

    val dcID = Utils.getDataContainerID(newDDF)
    val metaInfo = new MetaInfo("model", "String")
    return new DataFrameResult(dcID, Array(metaInfo))
  }
}
