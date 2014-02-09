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
}