package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDFManager, ATestSuite}
import com.adatao.ddf.{DDFManager, DDF}

/**
 * author: daoduchuan
 */
class KmeansSuite extends ATestSuite {

  test("Test Kmeans integation with mllib") {
    val manager= DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]
    createTableAirlineWithNA(sparkManager.getSharkContext)
    val ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airline")
    ddf.train(new Kmeans())
    manager.shutdown()
  }
}
