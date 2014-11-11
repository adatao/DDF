package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import io.ddf.ml.IModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/**
 * author: daoduchuan
 */
class DecisionTreeSuite extends ABigRClientTest {

  test("test decision tree") {
    createTableMtcars
    val cmd1 = new Sql2DataFrame("select * from mtcars", true)
    val r1 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](cmd1).result

    val dataContainerId = r1.dataContainerID
    val command = new DecisionTree(dataContainerId, Array(0,1,2,3,4,5), 7, "Classification",
      maxDepth = 10)
    val model = bigRClient.execute[DecisionTreeModel](command).result
    assert(model != null)
  }

  def generateOrderedLabeledPointsWithLabel0(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(0.0, Vectors.dense(i.toDouble, 1000.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPointsWithLabel1(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(1.0, Vectors.dense(i.toDouble, 999.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val label = if (i < 100) {
        0.0
      } else if (i < 500) {
        1.0
      } else if (i < 900) {
        0.0
      } else {
        1.0
      }
      arr(i) = new LabeledPoint(label, Vectors.dense(i.toDouble, 1000.0 - i))
    }
    arr
  }

  def generateCategoricalDataPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      if (i < 600) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
      } else {
        arr(i) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0))
      }
    }
    arr
  }
}
