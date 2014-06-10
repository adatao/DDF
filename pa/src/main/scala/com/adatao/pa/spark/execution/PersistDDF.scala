package com.adatao.pa.spark.execution

import com.adatao.ML.Utils
import com.adatao.basic.ddf.BasicDDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * author: daoduchuan
 */
class PersistDDF(dataContainerId: String) extends AExecutor[String]{

  protected override def runImpl(context: ExecutionContext): String = {
    val manager = context.sparkThread.getDDFManager;
    val ddfID = Utils.dcID2DDFID(dataContainerId)
    LOG.info(">>>>>>>>> DDF's uri = " + ddfID)
    val ddf = manager.getDDF(ddfID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Error persisting DDF", null)
    }
    if(ddf.isInstanceOf[BasicDDF]) {
      val uri = ddf.asInstanceOf[BasicDDF].persist()
      uri.toString
    } else {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error persisting DDF", null)
    }
  }
}
