package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.{SparkDDF, ATestSuite}
import shark.api.Row
import shark.memstore2.TablePartition

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
    assert(rddArrDouble.count() === 301)
    assert(rddArrObj.count() === 301)
  }
}
