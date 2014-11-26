package com.adatao.pa.spark.execution

/**
 * author: daoduchuan
 */
class PersistDDF(dataContainerID: String, overwrite: Boolean) extends AExecutor[String] {

  override def runImpl(ctx: ExecutionContext): String = {
    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID)
    val uri = ddf.getPersistenceHandler.persist(overwrite)
    uri.toString
  }
}
