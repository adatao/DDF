/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.{IHandleRepresentations, ARepresentationHandler}
import com.adatao.ddf.content.Schema.ColumnType
import java.lang.Class
import scala.reflect.Manifest
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import shark.api.Row
import com.adatao.spark.ddf.SparkDDFManager
import org.apache.spark.mllib.regression.LabeledPoint
import com.adatao.spark.ddf.content.SparkRepresentationHandler._
import com.adatao.ddf.exception.DDFException

/**
 * RDD-based SparkRepresentationHandler
 *
 * @author ctn
 *
 */
class SparkRepresentationHandler(container: SparkDDFManager) extends ARepresentationHandler(container) with IHandleRepresentations {

  /**
   *
   */
  protected def getRepresentationImpl(elementType: Class[_]): Object = {
    val schema = container.getSchemaHandler
    val numCols = schema.getNumColumns.toInt

    if (container.getRepresentationHandler.get(classOf[Row]) == null) {
      throw new Exception("Please load container representation")
    }

    val rdd = container.getRepresentationHandler.get(classOf[Row]).asInstanceOf[RDD[Row]]
    val extractors = schema.getColumns().map(colInfo => doubleExtractor(colInfo.getType)).toArray

    elementType match {

      case arrayObject if arrayObject == classOf[Array[Object]] => getRDDArrayObject(rdd, numCols)

      case arrayDouble if arrayDouble == classOf[Array[Double]] => getRDDArrayDouble(rdd, numCols, extractors)

      case labeledPoint if labeledPoint == classOf[LabeledPoint] => getRDDLabeledPoint(rdd, numCols, extractors)

      case _ => throw new DDFException("elementType not supported")
    }
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   */
  def set[T](data: RDD[T])(implicit m: Manifest[T]) = {
    this.reset
    this.add(data)
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones
   */
  def add[T](data: RDD[T])(implicit m: Manifest[T]): Unit = this.add(data, m.erasure)

  private def forAllReps[T](f: RDD[_] ⇒ Any) {
    mReps.foreach {
      kv ⇒ if (kv._2 != null) f(kv._2.asInstanceOf[RDD[_]])
    }
  }

  override def cacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          LOG.info(this.getClass() + ": Persisting " + rdd)
          rdd.persist
        }
    })
  }

  override def uncacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          LOG.info(this.getClass() + ": Unpersisting " + rdd)
          rdd.unpersist(false)
        }
    })
  }
}

object SparkRepresentationHandler {
  /**
   *
   */
  def getRDDArrayObject(rdd: RDD[Row], numCols: Int): RDD[Array[Object]] = {
    rdd.map {
      row => rowToArrayObject(row, numCols)
    }
  }

  def getRDDArrayDouble(rdd: RDD[Row], numCols: Int, extractors: Array[Object => Double]): RDD[Array[Double]] = {
    rdd.map(row => rowToArrayDouble(row, numCols, extractors)).filter(row => row != null)
  }

  def getRDDLabeledPoint(rdd: RDD[Row], numCols: Int, extractors: Array[Object => Double]): RDD[LabeledPoint] = {
    rdd.map(row => rowToLabeledPoint(row, numCols, extractors)).filter(point => point != null)
  }

  def rowToArrayObject(row: Row, numCols: Int): Array[Object] = {
    val array = new Array[Object](numCols)
    var i = 0
    while (i < numCols) {
      array(i) = row.getPrimitive(i)
      i += 1
    }
    array
  }

  def rowToArrayDouble(row: Row, numCols: Int, extractors: Array[Object => Double]): Array[Double] = {
    val array = new Array[Double](numCols)
    var i = 0
    var isNull = false

    while (i < numCols && isNull == false) {
      val obj = row.getPrimitive(i)
      if (obj == null) {
        isNull = true
      }
      else {
        array(i) = extractors(i)(obj)
      }
      i += 1
    }
    if (isNull) null else array
  }

  def rowToLabeledPoint(row: Row, numCols: Int, extractors: Array[Object => Double]): LabeledPoint = {
    val features = new Array[Double](numCols - 1)
    var label = 0.0
    var isNull = false
    var i = 0

    while (i < numCols && isNull == false) {
      val obj = row.getPrimitive(i)
      if (obj == null) {
        isNull = true
      }
      else {
        if (i < numCols - 1) {
          features(i) = extractors(i)(obj)
        }
        else {
          label = extractors(i)(obj)
        }
      }
      i += 1
    }
    if (isNull) null else new LabeledPoint(label, features)
  }

  def doubleExtractor(colType: ColumnType): Object => Double = colType match {
    case ColumnType.DOUBLE => {
      case obj => obj.asInstanceOf[Double]
    }

    case ColumnType.INTEGER => {
      case obj => obj.asInstanceOf[Int].toDouble
    }

    case ColumnType.STRING => {
      case _ => throw new Exception("Cannot convert string to double.")
    }
  }
}