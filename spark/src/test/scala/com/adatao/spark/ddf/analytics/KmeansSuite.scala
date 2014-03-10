package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri
import scala.collection.JavaConversions._

/**
  */
class KmeansSuite extends ATestSuite {

  test("Test Kmeans integation with mllib") {
    val manager = DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]

    createTableAirlineWithNA(sparkManager.getSharkContext)
    createTableAirline(sparkManager.getSharkContext)

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    val ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airline")

    val ddf2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val ddf3 = manager.sql2ddf("select " +
      "distance,arrdelay, depdelay from airlineWithNA")

    val ddf4 = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airlineWithNA")

    val model = ddf.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    val initialWeight = for {
      x <- 0 until (ddf2.getNumColumns - 1)
    } yield (math.random)
    /*
    val mlModel = ddf2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)

    val kmeansPred = model.predict(ddf4)
    val lmPred = mlModel.predict(ddf3)
    */
    val mlModel = ddf2.ML.train("logisticRegression", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
    // val lmPred = mlModel.predict(ddf3)
    val kmeansPred = model.predict(ddf4).asInstanceOf[SparkDDF]
    manager.shutdown()
  }
}
