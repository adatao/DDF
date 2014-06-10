package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.Utils
import com.adatao.pa.spark.DataManager.MetaInfo

/**
 * author: daoduchuan
 */
class LoadDDF(persistenceUri: String) extends AExecutor[DataFrameResult] {

  protected override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val ddf = manager.loadDDF(persistenceUri)
    manager.addDDF(ddf)
    val dcID = Utils.getDataContainerID(ddf)
    val metaInfo = new MetaInfo("model", "string")

    return new DataFrameResult(dcID, Array(metaInfo))
  }
}
