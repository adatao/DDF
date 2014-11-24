package com.adatao.pa.spark.execution

import io.spark.ddf.SparkDDFManager
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.sql.SchemaRDD
import io.ddf.DDF
import scala.collection.JavaConversions._
/**
 */

class PersistDDF(dataContainerID: String, tableName: String) extends AExecutor[Unit] {

  override def runImpl(ctx: ExecutionContext): Unit = {
    val manager = ctx.sparkThread.getDDFManager.asInstanceOf[SparkDDFManager]
    val ddf = manager.getDDF(dataContainerID)
    val hiveSchema = PersistDDF.createHiveSchema(ddf)
    manager.getHiveContext.sql(s"create table ${tableName} ($hiveSchema})")
    val schemaRDD = ddf.getRepresentationHandler.get(RepresentationHandler.SCHEMARDD.getTypeSpecsString).asInstanceOf[SchemaRDD]
    schemaRDD.insertInto(tableName)
  }
}

object PersistDDF {
  def createHiveSchema(ddf: DDF): String = {
    ddf.getSchema.getColumns.map{
      column => s"${column.getName} ${column.getType.toString}"
    }.mkString(", ")
  }
}
