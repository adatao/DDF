package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import org.apache.spark.rdd.RDD

/**
  */
class MllibIntegrationSuite extends ATestSuite {

  test("Test MLLib integation with mllib") {

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    val ddfTrain = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airline")

    val ddfPredict = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airlineWithNA")

    val ddfTrain2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")
    
    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val kmeansModel = ddfPredict.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    //for regression, need to add ONE for bias-term
    val initialWeight = for {
      x <- 0 until (ddfTrain2.getNumColumns-1)
    } yield (math.random)

    val regressionModel = ddfTrain2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
      
    
    val yTrueYpred = ddfPredict2.ML.applyModel(regressionModel, true, true)
    val yPred = ddfPredict.ML.applyModel(kmeansModel, false, true)

    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    yPred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    manager.shutdown()
  }
}
