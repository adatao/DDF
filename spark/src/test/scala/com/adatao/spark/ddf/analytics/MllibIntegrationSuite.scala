package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{ SparkDDF, SparkDDFManager, ATestSuite }
import com.adatao.ddf.DDFManager
import org.apache.spark.rdd.RDD
import java.util.HashMap

/**
 */
class MllibIntegrationSuite extends ATestSuite {

  ignore("Test MLLib integation with mllib") {
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

    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val kmeansModel = ddfPredict.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    //for regression, need to add ONE for bias-term
    val initialWeight = for {
      x <- 0 until (ddfTrain2.getNumColumns - 1)
    } yield (math.random)

    val regressionModel = ddfTrain2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)

    val yTrueYpred = ddfPredict2.ML.applyModel(regressionModel, true, true)
    val yPred = ddfPredict.ML.applyModel(kmeansModel, false, true)

    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    yPred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    manager.shutdown()
  }

  test("Test MLLib integation with mllib 222222") {
    val manager = DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]

    createTableAirlineWithNA(sparkManager.getSharkContext)
    createTableAirline(sparkManager.getSharkContext)

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    //for regression, need to add ONE for bias-term
    val initialWeight = for {
      x <- 0 until (ddfTrain3.getNumColumns - 1)
    } yield (math.random)

    System.setProperty("sparse.max.range", "10000")

    //construct columnSummary parameter
    var columnsSummary = new HashMap[String, Array[Double]]
    var hmin = new Array[Double](10)
    var hmax = new Array[Double](10)
    //convert columnsSummary to HashMap
    var i = 0
    while (i < 10) {
      hmin(i) = 1
      hmax(i) = 1
      i += 1
    }
    columnsSummary.put("min", hmin)
    columnsSummary.put("max", hmax)

    // fake the training with learningRate = 0.0
//    numIters: java.lang.Integer,
//    learningRate: java.lang.Double,
//    ridgeLambda: java.lang.Double,
//    initialWeights: scala.Array[Double],
//    numFeatures: Int,
//    columnsSummary: HashMap[String, Array[Double]]): LogisticRegressionModel = {

    //		val trainer = new LogisticRegressionCRS(dataContainerId, Array(0, 1), 2, columnsSummary, 1, 0.0, lambda, Array(37.285, -5.344, 1))

    val regressionModel = ddfTrain3.ML.train("logisticRegressionCRS", 1: java.lang.Integer, 0.1: java.lang.Double,0.1: java.lang.Double, Array(37.285, -5.344, -5.344, 1), 3: java.lang.Integer, columnsSummary)
    println(">>>>>> regressionModel=" + regressionModel.getRawModel())

    val yTrueYpred = ddfTrain3.ML.applyModel(regressionModel, true, true)
//
    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    manager.shutdown()
  }
}
