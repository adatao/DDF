package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.{SparkDDF, ATestSuite}
import shark.api.Row
import shark.memstore2.TablePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.junit.Assert.assertEquals
import scala.collection.JavaConversions._
import com.adatao.ddf.exception.DDFException

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
    val rddArrLP = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[LabeledPoint])

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

  test("Can do sql queries after CrossValidation ") {
    val ddf = manager.sql2ddf("select * from airline").asInstanceOf[SparkDDF]
    val tableName = ddf.getTableName
    for (split <- ddf.ML.CVKFold(5, 10)) {
      val train = split(0).asInstanceOf[SparkDDF]
      val test = split(1).asInstanceOf[SparkDDF]

      assert(train.getRepresentationHandler.has(classOf[RDD[_]], classOf[TablePartition]))
      assert(test.getRepresentationHandler.has(classOf[RDD[_]], classOf[TablePartition]))

      val tableName1 = train.getTableName.replace("-", "_")
      val tableName2 = test.getTableName.replace("-", "_")

      val ddf1 = train.getSqlHandler.sql2ddf(s"select month, year, dayofmonth from $tableName1")
      val ddf2 = test.getSqlHandler.sql2ddf(s"select * from $tableName2")

      assert(ddf1 != null)
      assert(ddf2 != null)
      assert(ddf1.getNumColumns == 3)
      assert(ddf1.getNumRows + ddf2.getNumRows == 301)
    }
  }

  test("Can handle null value with generic DDF") {
    val ddf = manager.sql2ddf("select year, month, dayofmonth from airline")

    val rddArrDouble = ddf.getRepresentationHandler().get(classOf[RDD[_]], classOf[Array[Double]])
    val rddLP = ddf.getRepresentationHandler().get(classOf[RDD[_]], classOf[LabeledPoint])
    val rddArrLP = ddf.getRepresentationHandler().get(RepresentationHandler.RDD_ARRAY_LABELED_POINT);

    val ArrArrDouble = rddArrDouble.asInstanceOf[RDD[Array[Double]]].collect()

    ArrArrDouble.foreach {
      row => assert(row(0) != 0.0, "row(0) == %s, expecting not 0.0".format(row(0)))
    }
    val count = rddLP.asInstanceOf[RDD[LabeledPoint]].count()
    val count1 = rddArrLP.asInstanceOf[RDD[LabeledPoint]].count()

    assertEquals(295, ArrArrDouble.size)
    assertEquals(295, count)
  }

  test("Handle empty DDF") {
    val ddf = manager.newDDF();
    try {
      ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[TablePartition])
      fail()
    } catch {
      case e: Throwable => {
        assert(e.getMessage == "DDF contains no representation")
      }
    }
  }

  test("Can do sql queries after Transform Rserve") {
    createTableMtcars()
    val ddf = manager.sql2ddf("select * from mtcars")
    val newDDF = ddf.Transform.transformNativeRserve("z1 = mpg / cyl, " +
      "z2 = disp * 0.4251437075, " +
      "z3 = rpois(nrow(df.partition), 1000)")

    val st= newDDF.Views.firstNRows(32)
    val ddf1 = manager.sql2ddf(s"select * from ${newDDF.getTableName}")

    assert(ddf1 != null)
  }
}
