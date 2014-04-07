package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager, ATestSuite}
import com.adatao.ddf.DDFManager
import com.adatao.ddf.content.Schema
import scala.collection.JavaConversions._
import org.junit.Assert._
import shark.api.Row

/**
  */
class CrossValidationSuite extends ATestSuite {

  val ddf2 = manager.sql2ddf("select * from airline")
  createTableAirline()

  test("random Split") {
    val arr = for {
      x <- 1 to 1000
    } yield (Array(x.asInstanceOf[Object]))

    val data = manager.getSparkContext.parallelize(arr, 2)
    val schema = new Schema("data", "v1 int");

    val ddf = new SparkDDF(manager, data, classOf[Row], manager.getNamespace, "data", schema)
    for (seed <- 1 to 5) {
      for (split <- ddf2.ML.CVRandom(5, 0.85, seed)) {
        val train = split(0).asInstanceOf[SparkDDF].getRDD(classOf[Row]).collect()
        val test = split(1).asInstanceOf[SparkDDF].getRDD(classOf[Row]).collect().toSet
        assertEquals(0.85, train.size / 1000.0, 0.025)
        assert(train.forall(x => !test.contains(x)), "train element found in test set!")
      }
    }

    ddf2.ML.CVRandom(5, 0.85, 12321312312L)
  }

  test("KFold Split") {
    val arr = for {
      x <- 1 to 5000
    } yield (Array(x.asInstanceOf[Object]))

    val data = manager.getSparkContext.parallelize(arr, 2)
    val schema = new Schema("data", "v1 int");

    val ddf = new SparkDDF(manager, data, classOf[Array[Object]], manager.getNamespace, "data", schema)
    for (seed <- 1 to 3) {
      val betweenFolds = scala.collection.mutable.ArrayBuffer.empty[Set[Array[Object]]]
      for (split <- ddf2.ML.CVKFold(5, seed)) {
        val train = split(0).asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect()
        val test = split(1).asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect().toSet
        assertEquals(0.8, train.size / 5000.0, 0.02)
        assert(train.forall(x => !test.contains(x)), "train element found in test set!")
        betweenFolds += test
      }
      for (pair <- betweenFolds.toArray.combinations(2)) {
        val Array(a, b) = pair
        assert(a.intersect(b).isEmpty, "test set accross folds are not disjoint!")
      }
    }

    ddf2.ML.CVKFold(5, 12321312312L)
  }
}
