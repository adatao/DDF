package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import io.ddf.DDF

/**
 * author: daoduchuan
 */
class LoadDDF(uri: String) extends AExecutor[DataFrameResult] {

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val manager = ctx.sparkThread.getDDFManager
    val dummyDDF = manager.newDDF()
    val persistenceHandler = dummyDDF.getPersistenceHandler
    val ddf = persistenceHandler.load(uri).asInstanceOf[DDF]

    manager.addDDF(ddf)
    new DataFrameResult(ddf)
  }
}
