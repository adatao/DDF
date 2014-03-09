package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.{ DDFManager, DDF }
import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import shark.api.Row
import org.apache.spark.mllib.regression.LassoModel
;
/**
 */
class KmeansSuite extends ATestSuite {

	test("Test Kmeans integation with mllib") {
		val manager = DDFManager.get("spark")
		val sparkManager = manager.asInstanceOf[SparkDDFManager]
    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")
		createTableAirlineWithNA(sparkManager.getSharkContext)
		val ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
			"distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
			"securitydelay, lateaircraftdelay from airline")

    val ddf2 = manager.sql2ddf("select " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay, delayed from airline_delayed")
    val ddf3 = manager.sql2ddf("select " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airlineWithNA")
		//ddf.ML.train("kmeans", int2Integer(2), int2Integer(100), int2Integer(10), "random")
    val predddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
      "distance,arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, " +
      "securitydelay, lateaircraftdelay from airlineWithNA")
    val model = ddf.ML.train("kmeans", 5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    val initialWeight2 = for{
      x <- 0 until (ddf2.getNumColumns - 1)
    }yield(math.random)

    val mlModel = ddf2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight2.toArray)

    val kmeansPred = model.predict(predddf).asInstanceOf[SparkDDF]
    val lmPred = mlModel.predict(ddf3)

    manager.shutdown()
	}
}
