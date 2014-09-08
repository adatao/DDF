package com.adatao.spark.ddf.analytics

import io.spark.ddf.SparkDDF
import io.ddf.content.Schema
import scala.collection.JavaConversions._
import org.junit.Assert._
import com.adatao.spark.ddf.ATestSuite
import io.ddf.types.Matrix
import io.ddf.types.Vector
import io.ddf.DDF

class TransformSuite extends ATestSuite {
  createTableAirlineSmall()

  test("dummy coding") {
    val ddf: DDF = manager.sql2ddf("select * from airline")

    val ddf2 = ddf.getTransformationHandler().dummyCoding(Array("origin"), "arrdelay")

    val rdd = ddf2.asInstanceOf[SparkDDF].getRDD(classOf[(Matrix, Vector)])
    val a = rdd.collect()
    var m = a(0)._1
    
    assertEquals(m.getRows(), 16, 0.0)
    assertEquals(m.getColumns(), 3, 0.0)
    //first row
    assertEquals(m(0, 0), 1.0, 0.0)
    assertEquals(m(0, 1), 0.0, 0.0)
    assertEquals(m(0, 2), 1.0, 0.0)
    
    //second row
    assertEquals(m(1, 2), 1.0, 0.0)
    
    //second partition
    m = a(1)._1
    //first row
    assertEquals(m(0, 0), 1.0, 0.0)
    assertEquals(m(0, 1), 0.0, 0.0)
    assertEquals(m(0, 2), 0.0, 0.0)
    
    assertEquals(m(3, 0), 1.0, 0.0)
    assertEquals(m(3, 1), 1.0, 0.0)
    assertEquals(m(3, 2), 0.0, 0.0)

    //arrdelay
    val n = a(0)._2
    assertEquals(n(0), -14.0, 0.0)

  }
}