package com.adatao.pa.spark.execution

import com.adatao.basic.ddf.content.PersistenceHandler
import com.adatao.basic.ddf.BasicDDF
import com.adatao.ddf.ml.Model

/**
 * author: daoduchuan
 */
class LoadModel(uri: String) extends AExecutor[Object] {
  override def runImpl(ctx: ExecutionContext): Object = {
    val manager = ctx.sparkThread.getDDFManager
    val persistenceHandler = new PersistenceHandler(null);
    val modelDDF = persistenceHandler.load(uri).asInstanceOf[BasicDDF]
    val model = Model.deserializeFromDDF(modelDDF);
    model.getRawModel
  }
}
