package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.basic.ddf.BasicDDF
import com.adatao.pa.spark.Utils
import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
  */


class Serialize2DDF(modelID: String) extends AExecutor[DataFrameResult] {

  protected override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val model = manager.getModel(modelID)
    if(model == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "model not founded", null);
    }

    val newDDF = new BasicDDF(model)
    val name = manager.addDDF(newDDF)
    LOG.info(">>>>> put ddf to manager: " + name)

    val dcID = Utils.getDataContainerID(newDDF)
    val metaInfo = new MetaInfo("model", "String")
    return new DataFrameResult(dcID, Array(metaInfo))
  }
}
