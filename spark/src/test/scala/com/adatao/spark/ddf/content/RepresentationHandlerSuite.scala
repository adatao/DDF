package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import shark.api.Row
import shark.memstore2.TablePartition

/**
 * author: daoduchuan
 */
class RepresentationHandlerSuite extends ATestSuite {

  test("  ") {
    val manager = DDFManager.get("spark").asInstanceOf[SparkDDFManager]
    createTableAirline(manager.getSharkContext)
    val ddf = manager.sql2ddf("select * from airline").asInstanceOf[SparkDDF]
    val rddRow = ddf.getRDD(classOf[Row])
    val rddTablePartition = ddf.getRDD(classOf[TablePartition])

    assert(rddRow != null, "Can get RDD[Row]")
    assert(rddTablePartition != null, "Can get RDD[TablePartition]")
    assert(rddRow.count() == 301)
    rddTablePartition.count()
    /*rddTablePartition.foreach{
      tp => assert(tp.isInstanceOf[TablePartition])
    } */
  }
}
