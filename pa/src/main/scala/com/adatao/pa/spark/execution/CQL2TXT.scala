package com.adatao.pa.spark.execution

import java.util.List
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

class CQL2TXT(cqlCommand: String) extends AExecutor[List[String]] {

  override def runImpl(context: ExecutionContext): List[String] = {
    val manager = context.sparkThread.getDDFManager
    val res = manager.sql2txt(cqlCommand, "dse")
    if(res == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error", null)
    } else {
      res
    }
  }
}
