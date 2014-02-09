package com.adatao.ddf.spark

import org.apache.spark.SparkContext
import com.adatao.ddf.DDF

class SparkDDFFactory(val sc: SparkContext) extends com.adatao.ddf.DDFFactory {
  def createSchema(schemaString: java.lang.String): DDF = {
    return null
  }

  def fromSql(sqlCommand: java.lang.String): DDF = {
    return null
  }
}