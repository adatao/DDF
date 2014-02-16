package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDFManager, ATestSuite}

/**
 * author: daoduchuan
 */
class KmeansSuite extends ATestSuite{

  test("Test Kmeans integation with mllib") {
    val ddfManager= new SparkDDFManager()
    val ddf= ddfManager.cmd2ddf("Select * from airline")
    val model= ddf.runAlgorithm(new Kmeans()).asInstanceOf[KmeansModel]
    ddfManager.shutdown()

  }
}
