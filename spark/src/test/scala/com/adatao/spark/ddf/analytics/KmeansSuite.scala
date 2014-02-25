package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDF

/**
 * author: daoduchuan
 */
class KmeansSuite extends ATestSuite {

  test("Test Kmeans integation with mllib") {
    DDF.setEngine("spark")
    val manager = DDF.getDefaultManager.asInstanceOf[SparkDDFManager]
    createTableAirlineWithNA(manager.getSharkContext)
    val ddf = DDF.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airline")
    ddf.train(new Kmeans())
    DDF.shutdown()
  }
}
