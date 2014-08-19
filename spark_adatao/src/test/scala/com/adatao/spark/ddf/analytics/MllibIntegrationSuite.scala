package com.adatao.spark.ddf.analytics

import io.spark.ddf.{ SparkDDF, SparkDDFManager }
import io.ddf.DDFManager
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import java.util.HashMap
import com.adatao.spark.ddf.ATestSuite

/**
 */
class MllibIntegrationSuite extends ATestSuite {

//  ignore("Test MLLib integation with mllib") {
//    val manager = DDFManager.get("spark")
//    val sparkManager = manager.asInstanceOf[SparkDDFManager]
//
//    createTableAirlineWithNA
//    createTableAirline
//
//    manager.sql2txt("drop table if exists airline_delayed")
//    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")
//
//    val ddfTrain = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
//      "distance,arrdelay, depdelay from airline")
//
//    val ddfPredict = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
//      "distance,arrdelay, depdelay from airlineWithNA")
//
//    val ddfTrain2 = manager.sql2ddf("select " +
//      "distance, arrdelay, depdelay, delayed from airline_delayed")
//
//    //for glm
//    val ddfTrain3 = manager.sql2ddf("select " +
//      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")
//
//    val ddfPredict2 = manager.sql2ddf("select " +
//      "distance, arrdelay, depdelay, delayed from airline_delayed")
//
//    val kmeansModel = ddfPredict.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")
//
//    //for regression, need to add ONE for bias-term
//    val initialWeight = for {
//      x <- 0 until (ddfTrain2.getNumColumns - 1)
//    } yield (math.random)
//
//    val regressionModel = ddfTrain2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
//      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
//
//    val yTrueYpred = ddfPredict2.ML.applyModel(regressionModel, true, true)
//    val yPred = ddfPredict.ML.applyModel(kmeansModel, false, true)
//
//    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
//    yPred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
//    manager.shutdown()
//  }

  test("Test MLLib integation with mllib 222222") {
//    val manager = DDFManager.get("spark")
//    val sparkManager = manager.asInstanceOf[SparkDDFManager]

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val ddfTrain4 = manager.sql2ddf("select " +
      "distance, depdelay, if (arrdelay > 10.89, 1, 0) as delayed from airline_delayed")
      
    val kmeansModel = ddfPredict2.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    //for regression, need to add ONE for bias-term
    val initialWeight = for {
      x <- 0 until (ddfTrain3.getNumColumns - 1)
    } yield (math.random)

    
    
    val regressionModel = ddfTrain3.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
    val yTrueYpred = ddfPredict2.ML.applyModel(regressionModel, true, true)
    val yPred = ddfPredict2.ML.applyModel(kmeansModel, false, true)
    val nrows = yTrueYpred.VIEWS.head(10)
    println("YTrue YPred")
    for (x <- nrows) println(x)

    // numIterations = 10, stepSize = 0.1
    val logRegModel = ddfTrain4.ML.train("logisticRegressionWithSGD", 10: java.lang.Integer, 0.1: java.lang.Double)
    val yPred1 = ddfTrain4.ML.applyModel(logRegModel, false, true)
     
    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    yPred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    
    println(yPred1.asInstanceOf[SparkDDF].getNumRows())

    
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

    val regressionModel2 = ddfTrain3.ML.train("logisticRegressionCRS", 1: java.lang.Integer, 0.1: java.lang.Double,0.1: java.lang.Double, Array(37.285, -5.344, -5.344, 1), 3: java.lang.Integer, columnsSummary)
    
    val yTrueYpred2 = ddfTrain3.ML.applyModel(regressionModel2, true, false)
    val a = ddfTrain3.getMLMetricsSupporter().roc(yTrueYpred2, 10)
    
    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    manager.shutdown()
  }
}