package com.adatao.ddf.spark.content

import shark.memstore2.TablePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.jblas.{DoubleMatrix}
/**
 * Created with IntelliJ IDEA.
 * @author daoduchuan
 */
object TablePartitionHelper {
  def getArrayObject(cols: Array[Int], rdd: RDD[TablePartition]): RDD[Array[Object]] = {

    null
  }

  def getArrayDouble(cols: Array[Int], rdd: RDD[TablePartition]): RDD[Array[Double]] = {

    null
  }

  def getLabeledPoint(cols: Array[Int], rdd: RDD[TablePartition]): RDD[LabeledPoint] = {

    null
  }

  def getLabeledPoints(cols: Array[Int], rdd: RDD[TablePartition]): RDD[(DoubleMatrix, DoubleMatrix)] = {

    null
  }
}
