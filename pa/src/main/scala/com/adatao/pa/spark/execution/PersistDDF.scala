package com.adatao.pa.spark.execution

import io.spark.ddf.SparkDDFManager
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.sql.SchemaRDD

/**
 */

class PersistDDF(dataContainerID: String, tableName: String) extends AExecutor[Unit] {

  override def runImpl(ctx: ExecutionContext): Unit = {
    val manager = ctx.sparkThread.getDDFManager.asInstanceOf[SparkDDFManager]
    val ddf = manager.getDDF(dataContainerID)
    val schemardd = ddf.getRepresentationHandler.get(RepresentationHandler.SCHEMARDD.getTypeSpecsString).asInstanceOf[SchemaRDD]
    schemardd.saveAsTable(tableName)
  }
}
