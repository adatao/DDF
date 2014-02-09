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
    val driver = DDFDriverManager.getDriver("spark:")
    val props = new java.util.HashMap[String, String]
    props.put("spark.home", System.getProperty("SPARK_HOME"))
    println(driver)
    val ddfFactory = driver.connect("spark://ubuntu:7077", props)
    println(ddfFactory)
  }
}