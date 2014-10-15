package com.adatao.spark.ddf.etl

import io.spark.ddf.etl.{SqlHandler => IOSqlHandler}
import io.ddf.DDF
import io.ddf.content.Schema
import com.adatao.spark.ddf.SparkDDF

/**
 */
class SqlHandler(mDDF: DDF) extends IOSqlHandler(mDDF) {

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: Schema.DataFormat): DDF = {
    val ddf = super.sql2ddf(command, schema, dataSource)
    ddf.asInstanceOf[SparkDDF].cacheTable()
    ddf
  }
}
