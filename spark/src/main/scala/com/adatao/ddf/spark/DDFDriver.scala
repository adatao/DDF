package com.adatao.ddf.spark

import com.adatao.ddf.DDFDriver
import com.adatao.ddf.DDF
import com.adatao.ddf.DDFDriverManager
import com.adatao.ddf.DDFFactory
import com.adatao.ddf.exception.DDFException
import org.apache.spark.SparkContext
import collection.JavaConversions._

class DDFDriver extends com.adatao.ddf.DDFDriver {
  DDFDriver // to trigger the static call to register the driver with driver manager.
  
  def acceptURL(connectionURL: java.lang.String): Boolean = {
    // set the spark context using the connection URL
    
    return true
  }
  
  def acceptURL(connectionURL: java.lang.String, connectionProps: java.util.Map[java.lang.String, java.lang.String]): Boolean = {
    // set the spark context using the connection URL
    return true
  }
  
  def connect(connectionURL: java.lang.String, 
      connectionProps: java.util.Map[java.lang.String, java.lang.String]): DDFFactory = {
    val sc = new SparkContext(master = connectionURL, sparkHome = connectionProps.get("spark.home"),
        environment = connectionProps, appName = "DDF Client")
    new SparkDDFFactory(sc)
  }
}

object DDFDriver {
  try {
    DDFDriverManager.registerDDFDriver(new com.adatao.ddf.spark.DDFDriver)
  } catch {
    case ex: DDFException => println(ex.getMessage)
  }
}