package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.{SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager

/**
 * author: daoduchuan
 */
class RepresentationHandlerSuite extends ATestSuite {

  test("  ") {
    val manager = DDFManager.get("spark").asInstanceOf[SparkDDFManager]
    createTableAirline(manager.getSharkContext)
    val ddf = manager.sql2ddf("select * from airline")
  }
}
