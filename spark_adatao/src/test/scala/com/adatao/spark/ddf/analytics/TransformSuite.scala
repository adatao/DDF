package com.adatao.spark.ddf.analytics

import io.spark.ddf.SparkDDF
import io.ddf.content.Schema
import scala.collection.JavaConversions._
import org.junit.Assert._
import com.adatao.spark.ddf.ATestSuite

import io.ddf.DDF
import com.adatao.spark.ddf.etl.TransformationHandler
import io.ddf.types.TupleMatrixVector
import com.adatao.spark.ddf.etl.TransformationHandler._
import io.ddf.types.Vector
import org.apache.spark.sql.columnar.{NativeColumnAccessor, ColumnAccessor, CachedBatch}
import java.nio.ByteBuffer
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, GenericRow}

class TransformSuite extends ATestSuite {

  createTableAirlineSmall()
  test("dummy coding") {
    val ddf: DDF = manager.sql2ddf("select * from airline")

    val ddf2 = (ddf.getTransformationHandler()).dummyCoding(Array("origin"), "arrdelay")

    val rdd = ddf2.asInstanceOf[SparkDDF].getRDD(classOf[TupleMatrixVector])
    val a = rdd.collect()
    val rdd2 = ddf.asInstanceOf[SparkDDF].getRDD(classOf[CachedBatch])

    val collection = rdd2.collect()
    collection.map{
      case CachedBatch(buffers, row) => {
        println("buffer.count = " + buffers.size)
        println("buffers(0). count = " + buffers(0).size)
        println("row = " + row.mkString(", "))
        val columnAccessors = buffers.map{buffer => ColumnAccessor(ByteBuffer.wrap(buffer))}

        val colAccessor = columnAccessors(3).asInstanceOf[NativeColumnAccessor[_]]
        val colType = colAccessor.columnType
        val buffer = colAccessor.buffer
        println(">>>> colType = " + colType.toString())
        val mutableRow = new GenericMutableRow(1)
        while(colAccessor.hasNext) {
           colAccessor.extractSingle(mutableRow, 0)
           val value = mutableRow.get(0)
           println(">>>> value = " + value)
        }
      }
    }

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
  test("test ytrueypred") {
    //createTableAirline
    class DummyModel(weights: Vector, numSamples: Long) extends ALinearModel[Double](weights, numSamples) {
      override def predict(features: Vector) = {
        this.linearPredictor(features)
      }
    }

    val ddf = manager.sql2ddf("select * from airline")
    val dummyCodingDDF = ddf.getTransformationHandler.dummyCoding(Array("year", "month", "dayofmonth"), "arrdelay")

    val model = new DummyModel(Vector(Array(0.5,0.1, 0.2, 0.3)), 100)
    val rddMatrixVector = dummyCodingDDF.asInstanceOf[SparkDDF].getRDD(classOf[TupleMatrixVector])
    assert(rddMatrixVector.count() > 0)
    val rdd = model.yTrueYPred(rddMatrixVector)
    assert(rdd.count() == 31)
  }

  test("test DummyCoding handling NA") {
    createTableAirlineWithNA()
    val ddf = manager.sql2ddf("select * from airlineWithNA")
    val ddf2 = (ddf.getTransformationHandler()).dummyCoding(Array("year"), "arrdelay")
    val rdd = ddf2.asInstanceOf[SparkDDF].getRDD(classOf[TupleMatrixVector])
    rdd.count
  }

  test("Test TransformDummy with NA") {

  }
}
