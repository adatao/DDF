package com.adatao.pa.spark.execution

/**
 * author: daoduchuan
 */
class PersistModel(modelID: String, modelAlias: String) extends AExecutor[String] {

  override def runImpl(ctx: ExecutionContext): String = {
    val manager = ctx.sparkThread.getDDFManager;
    val model = manager.getModel(modelID)
    model.setName(modelAlias)

    val ddf = model.serialize2DDF(manager)
    val uri = ddf.persist()
    uri.toString
  }
}
