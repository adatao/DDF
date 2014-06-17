package com.adatao.pa.spark.execution

/**
 * author: daoduchuan
 */
class PersistModel(modelID: String) extends AExecutor[String] {

  override def runImpl(ctx: ExecutionContext): String = {
    val manager = ctx.sparkThread.getDDFManager;
    val model = manager.getModel(modelID)
    val trainedCols = model.getTrainedColumns
    LOG.info(">>>>>> trainedCols = " + trainedCols.mkString(", "))
    val ddf = model.serialize2DDF(manager)
    val uri = ddf.persist()
    uri.toString
  }
}
