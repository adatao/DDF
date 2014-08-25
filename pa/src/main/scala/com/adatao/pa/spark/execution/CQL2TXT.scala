package com.adatao.pa.spark.execution

import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

class CQL2TXT(cqlCommand: String) extends AExecutor[String] {
  override def rumImpl(context: ExecutionContext): String = {
    val manager = context.sparkThread.getDDFManager();
    val res = manager.sql2txt(cqlCommand, "dse")
    if (res == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error", null)
    } else {
      res
    }
  }

}