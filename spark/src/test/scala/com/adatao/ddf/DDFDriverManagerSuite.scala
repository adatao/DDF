package com.adatao.ddf

import com.adatao.ddf.DDFDriver
import com.adatao.ddf.DDF
import com.adatao.ddf.DDFDriverManager

class DDFDriverManagerSuite extends ATestSuite {
  test("Test loading Spark Driver Manager") {
    println("Test me")
    new com.adatao.ddf.spark.DDFDriver
    val driver = DDFDriverManager.getDriver("spark:")
    println(driver)
  }
  
  test("Test connecting to a Spark Master") {
    println("Test me")
    new com.adatao.ddf.spark.DDFDriver
    val connStr = "spark://ubuntu:7077"
    val driver = DDFDriverManager.getDriver(connStr)
    val props = new java.util.HashMap[String, String]
    props.put("spark.home", System.getProperty("SPARK_HOME"))
    println(driver)
    // two ways of getting DDFFactory
    // first, get it through ddf driver
    val ddfFactory = driver.connect(connStr, props)
    println(ddfFactory)
    
    // second, get it through Drivermanager directly.
    val ddfFactory2 = DDFDriverManager.getDDFFactory(connStr, props)
  }
}