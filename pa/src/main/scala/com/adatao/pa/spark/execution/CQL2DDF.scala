package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.AdataoException

/**
 * author: daoduchuan
 */
class CQL2DDF(cqlCommand: String) extends AExecutor[DataFrameResult] {

  override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val ddf = manager.sql2ddf(cqlCommand, "dse")
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error", null)
    } else {
      new DataFrameResult(ddf)
    }
  }
}
