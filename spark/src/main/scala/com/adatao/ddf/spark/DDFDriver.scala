package com.adatao.ddf.spark

import com.adatao.ddf.DDFDriver
import com.adatao.ddf.DDF
import com.adatao.ddf.DDFDriverManager
import com.adatao.ddf.exception.DDFException

class DDFDriver extends com.adatao.ddf.DDFDriver {
  DDFDriver // to trigger the static call to register the driver with driver manager.
  
  def createSchema(schemaString: java.lang.String): DDF = {
    return null
  }

  def fromSql(sqlCommand: java.lang.String): DDF = {
    return null
  }
  
  def acceptURL(connectionURL: java.lang.String): Boolean = {
    return true
  }
}

object DDFDriver {
  try {
    DDFDriverManager.registerDDFDriver(new com.adatao.ddf.spark.DDFDriver)
  } catch {
    case ex: DDFException => println(ex.getMessage)
  }
}