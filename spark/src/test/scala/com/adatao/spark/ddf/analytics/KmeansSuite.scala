package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDFManager, ATestSuite}
import com.adatao.ddf.{DDFManager, DDF}

/**
 */
class KmeansSuite extends ATestSuite {

  test("Test Kmeans integation with mllib") {
    val manager= DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]
    createTableAirlineWithNA(sparkManager.getSharkContext)
    createTableAirline(sparkManager.getSharkContext)
    val ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airlineWithNA")
    val model = ddf.ML.train(new Kmeans())

    val ddf2 = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airline")
    model.predict(ddf2)

    manager.shutdown()
  }
}
