package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri
import scala.collection.JavaConversions._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

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

    val ddfTrain = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airline")

    val ddfPredict = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airlineWithNA")

    val ddfTrain2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance,arrdelay, depdelay from airline")

    val model = ddfTrain.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    val initialWeight = for {
      x <- 0 until (ddfTrain2.getNumColumns - 1)
    } yield (math.random)

    val mlModel = ddfTrain2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)

    val result1 = ddfTrain.ML.predict(model)
    val result2 = ddfPredict2.ML.predict(mlModel)

    val rdd= result1.asInstanceOf[SparkDDF].getRDD(classOf[java.lang.Integer]).count()

    result2.asInstanceOf[SparkDDF].getRDD(classOf[java.lang.Double]).count()
    //val kmeansPred = model.predict(ddfPredict)
    //val lmPred = mlModel.predict(ddfPredict2)
    LinearRegressionWithSGD
    manager.shutdown()
  }
}
