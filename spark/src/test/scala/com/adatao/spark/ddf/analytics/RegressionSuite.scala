package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import org.apache.spark.rdd.RDD
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import java.util.HashMap


class RegressionSuite extends ATestSuite {

  ignore("Logistic Regression with sparse input") {
    val manager = DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    
    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    
    val initialWeight = for {
      x <- 0 until (ddfTrain3.getNumColumns)
    } yield (math.random)

    //manual input
    var columnsSummary =  new HashMap[String, Array[Double]]
    var hmin = new Array[Double] (ddfTrain3.getNumColumns)
    var hmax = new Array[Double] (ddfTrain3.getNumColumns)
    //convert columnsSummary to HashMap
    var i = 0
    while(i < ddfTrain3.getNumColumns) {
      hmin(i) = 1
      hmax(i) = 10      
      i += 1
    }
    columnsSummary.put("min", hmin)
    columnsSummary.put("max", hmax)
    
    
    val glmModel = ddfTrain3.ML.train("logisticRegressionCRS", 10: java.lang.Integer,
    0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray : scala.Array[Double], ddfTrain3.getNumColumns: java.lang.Integer, columnsSummary)
    

    manager.shutdown()
  }
	
	test("Logistic Regression IRLS") {
    /*createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")
    
    
    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")
      
      val initialWeight = Array.fill(4){0.0}

    val glmModel = ddfTrain3.ML.train("logisticRegressionIRLS", ddfTrain3.getNumColumns(): java.lang.Integer, 
    		25: java.lang.Integer, 1e-8: java.lang.Double, 0: java.lang.Double, 
    		initialWeight.toArray: scala.Array[Double], false: java.lang.Boolean)
    
    val model: com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel = glmModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel]
    println(">>>>>>>>>>>>>>>>>>>")
    println(model.getWeights)
    assert(truncate(model.getWeights()(0), 2) === -1.92)
    println(model.getDeviance)
    assert(truncate(model.getDeviance, 2) === 265.91)
    println(model.getStdErrs)
    assert(truncate(model.getStdErrs()(0), 2) === 0.28)*/
    
    
    createTableAdmission()
    val ddfTrain3 = manager.sql2ddf("select v2, v3, v4, v1 from admission")
      
      val initialWeight = Array.fill(4){0.0}

    val glmModel = ddfTrain3.ML.train("logisticRegressionIRLS", ddfTrain3.getNumColumns(): java.lang.Integer, 
        25: java.lang.Integer, 1e-8: java.lang.Double, 0: java.lang.Double, 
        initialWeight.toArray: scala.Array[Double], false: java.lang.Boolean)
    
    val model: com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel = glmModel.getRawModel().asInstanceOf[com.adatao.spark.ddf.analytics.IRLSLogisticRegressionModel]
    
    println(">>>>>>>>>>>>>>>>>>>")
    println(model.getWeights)
    assert(truncate(model.getWeights()(0), 2) === -3.45)
    println(model.getDeviance)
    assert(truncate(model.getDeviance, 2) === 459.44)
    println(model.getStdErrs)
    assert(truncate(model.getStdErrs()(0), 2) === 1.13)
    assert(model.getNumIters === 4)
    
    manager.shutdown()
  }

  test("Linear Regression with Normal Equation") {
    val manager = DDFManager.get("spark")
    val sparkManager = manager.asInstanceOf[SparkDDFManager]

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    
    //for glm
    val ddfTrain = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay from airline")
    
    val initialWeight = for {
      x <- 0 until (ddfTrain.getNumColumns)
    } yield (math.random)

    //manual input
    var columnsSummary =  new HashMap[String, Array[Double]]
    var hmin = new Array[Double] (ddfTrain.getNumColumns)
    var hmax = new Array[Double] (ddfTrain.getNumColumns)
    //convert columnsSummary to HashMap
    var i = 0
    while(i < ddfTrain.getNumColumns) {
      hmin(i) = 1
      hmax(i) = 10      
      i += 1
    }
    columnsSummary.put("min", hmin)
    columnsSummary.put("max", hmax)
    
    
    val glmModel = ddfTrain.ML.train("linearRegressionNQ", 7: java.lang.Integer,
    0.1: java.lang.Double)

    println("Json model")
    val rawModel = glmModel.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.NQLinearRegressionModel]
    println(rawModel.weights.toJson)

    manager.shutdown()
  }
}
