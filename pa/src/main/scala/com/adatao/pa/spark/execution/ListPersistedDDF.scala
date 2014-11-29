package com.adatao.pa.spark.execution
import io.spark.ddf.content.PersistenceHandler
/**
 * author: daoduchuan
 */
class ListPersistedDDF extends AExecutor[Array[String]] {

  override def runImpl(ctx: ExecutionContext): Array[String] = {

    val manager = ctx.sparkThread.getDDFManager
    val dummyDDF = manager.newDDF()
    val listUris = dummyDDF.getPersistenceHandler.asInstanceOf[PersistenceHandler].listPersistedDDFUris
    listUris.foreach{
      str => LOG.info(">>> uri = " + str)
    }
    listUris.toArray
  }
}
