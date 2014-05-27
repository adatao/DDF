package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.{SparkDDF, ATestSuite}
import shark.api.Row
import shark.memstore2.TablePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.junit.Assert.assertEquals

/**
  */
class RepresentationHandlerSuite extends ATestSuite {
  createTableAirline()

  test("Can get RDD[Row] and RDD[TablePartition]") {
    val ddf = manager.sql2ddf("select * from airline").asInstanceOf[SparkDDF]
    val rddRow = ddf.getRDD(classOf[Row])
    val rddTablePartition = ddf.getRDD(classOf[TablePartition])
    assert(rddRow != null, "Can get RDD[Row]")
    assert(rddTablePartition != null, "Can get RDD[TablePartition]")
    assert(rddTablePartition.first().isInstanceOf[TablePartition])
    assert(rddRow.first().isInstanceOf[Row])
  }


  test("Can get RDD[Array[Double]] and RDD[Array[Object]]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]

    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    val rddArrObj = ddf.getRDD(classOf[Array[Object]])

    assert(rddArrDouble != null, "Can get RDD[Array[Double]]")
    assert(rddArrObj != null, "Can get RDD[Array[Object]]")
    assert(rddArrDouble.count() === 295)
    assert(rddArrObj.count() === 301)
  }
  test("Has representation after creating it") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val repHandler = ddf.getRepresentationHandler
    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    val rddArrObj = ddf.getRDD(classOf[Array[Object]])
    val rddArrLP = ddf.getRDD(classOf[LabeledPoint])

    assert(repHandler.has(classOf[RDD[_]], classOf[TablePartition]))
    assert(repHandler.has(classOf[RDD[_]], classOf[Array[Double]]))
    assert(repHandler.has(classOf[RDD[_]], classOf[Array[Object]]))
    assert(repHandler.has(classOf[RDD[_]], classOf[LabeledPoint]))
    assert(repHandler.has(classOf[RDD[_]], classOf[Row]))
  }

  test("Can handle null value") {
    val ddf = manager.sql2ddf("select year, month, dayofmonth from airline").asInstanceOf[SparkDDF]

    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    val rddArrLP = ddf.getRDD(classOf[LabeledPoint])

    val ArrArrDouble = rddArrDouble.collect()

    ArrArrDouble.foreach {
      row => assert(row(0) != 0.0, "row(0) == %s, expecting not 0.0".format(row(0)))
    }
    val count = rddArrLP.count()

    assertEquals(295, ArrArrDouble.size)
    assertEquals(295, count)
  }
}
