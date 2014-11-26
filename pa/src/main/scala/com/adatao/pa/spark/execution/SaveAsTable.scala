package com.adatao.pa.spark.execution

import io.spark.ddf.SparkDDFManager
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.sql.SchemaRDD
import io.ddf.DDF
import scala.collection.JavaConversions._
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 */

class SaveAsTable(dataContainerID: String, tableName: String) extends AExecutor[Unit] {

  override def runImpl(ctx: ExecutionContext): Unit = {
    val manager = ctx.sparkThread.getDDFManager.asInstanceOf[SparkDDFManager]
    val ddf = manager.getDDF(dataContainerID)
    val hiveSchema = SaveAsTable.createHiveSchema(ddf)
    try {
      manager.getHiveContext.sql(s"create table ${tableName} ($hiveSchema)")
      val schemaRDD = ddf.getRepresentationHandler.get(RepresentationHandler.SCHEMARDD.getTypeSpecsString).asInstanceOf[SchemaRDD]
      schemaRDD.insertInto(tableName)
    } catch {
      case e: org.apache.spark.sql.execution.QueryExecutionException =>
        throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, s"Table $tableName exists, please choose other table name", null)
    }
  }
}

object SaveAsTable {
  def createHiveSchema(ddf: DDF): String = {
    ddf.getSchema.getColumns.map{
      column => s"${column.getName} ${column.getType.toString}"
    }.mkString(", ")
  }
}
