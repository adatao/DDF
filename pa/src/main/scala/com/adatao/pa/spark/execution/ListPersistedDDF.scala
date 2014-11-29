package com.adatao.pa.spark.execution
import io.spark.ddf.content.PersistenceHandler
/**
 * author: daoduchuan
 */
class ListPersistedDDF extends AExecutor[java.util.List[String]] {

  override def runImpl(ctx: ExecutionContext): java.util.List[String] = {

    val manager = ctx.sparkThread.getDDFManager
    val dummyDDF = manager.newDDF()
    dummyDDF.getPersistenceHandler.asInstanceOf[PersistenceHandler].listPersistedDDF
  }
}
