/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.Schema.ColumnType
import com.adatao.ddf.content.Schema.Column
import com.adatao.ddf.content.Schema
import java.lang.Class
import scala.reflect.Manifest
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.JavaConversions._
import shark.api.Row
import com.adatao.ddf.DDF
import com.adatao.spark.ddf.content.RepresentationHandler._
import com.adatao.ddf.exception.DDFException
import org.apache.spark.mllib.regression.LabeledPoint
import com.adatao.ddf.content.{ RepresentationHandler ⇒ RH }
import com.adatao.ddf.content.RepresentationHandler.NativeTable
import shark.memstore2.TablePartition
import org.apache.spark.api.java.function.Function
import org.rosuda.REngine._
import org.apache.hadoop.io.{ Text, IntWritable }
import org.apache.hadoop.hive.serde2.io.DoubleWritable

/**
 * RDD-based SparkRepresentationHandler
 *
 */

class RepresentationHandler(mDDF: DDF) extends RH(mDDF) {

  override def getDefaultDataType: Array[Class[_]] = Array(classOf[RDD[_]], classOf[Row])

  /**
   * Converts from an RDD[Row] to any representation
   */
  override def createRepresentation(typeSpecs: String): Object = this.fromRDDRow(typeSpecs)

  protected def fromRDDRow(typeSpecs: String): Object = {
    val schemaHandler = mDDF.getSchemaHandler
    val numCols = schemaHandler.getNumColumns.toInt
    val srcRdd = this.toRDDRow
    val mappers: Array[Object ⇒ Double] = (schemaHandler.getColumns.map(column ⇒ getDoubleMapper(column.getType))).toArray

    typeSpecs match {
      case RDD_REXP ⇒ tablePartitionsToRDataFrame(rowsToTablePartitions(srcRdd), schemaHandler.getColumns)
      case RDD_TABLE_PARTITION ⇒ rowsToTablePartitions(srcRdd)
      case RDD_ARRAY_OBJECT ⇒ rowsToArraysObject(srcRdd)
      case RDD_ARRAY_DOUBLE ⇒ rowsToArraysDouble(srcRdd, mappers)
      case RDD_LABELED_POINT ⇒ rowsToLabeledPoints(srcRdd, mappers)
      case RH.NATIVE_TABLE ⇒ rowsToNativeTable(mDDF, srcRdd, numCols)
      case _ ⇒ throw new DDFException(String.format("TypeSpecs %s not supported. It must be one of:\n - %s\n - %s\n - %s\n - %s\n - %s\n -%s",
        typeSpecs,
        RDD_REXP, RDD_TABLE_PARTITION, RDD_ARRAY_OBJECT, RDD_ARRAY_DOUBLE, RDD_LABELED_POINT, RH.NATIVE_TABLE))
    }
  }

  /**
   * Converts to an RDD[Row] from any representation
   */
  protected def toRDDRow: RDD[Row] = {
    val rdd = this.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    if (rdd != null) return rdd

    // Try to convert from any known representation

    val nativeTable = this.get(classOf[NativeTable]).asInstanceOf[NativeTable]
    if (nativeTable != null) {
      return null // TODO
    }

    val arraysObject = this.get(classOf[RDD[_]], classOf[Array[_]], classOf[Object]).asInstanceOf[RDD[Array[Object]]]
    if (arraysObject != null) {
      return null // TODO
    }

    val arraysDouble = this.get(classOf[RDD[_]], classOf[Array[_]], classOf[Double]).asInstanceOf[RDD[Array[Double]]]
    if (arraysDouble != null) {
      return null // TODO
    }
    LabeledPoint
    val labeledPoints = this.get(classOf[RDD[_]], classOf[LabeledPoint]).asInstanceOf[RDD[LabeledPoint]]
    if (labeledPoints != null) {
      return null // TODO
    }

    null.asInstanceOf[RDD[Row]]
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
  def add[T](data: RDD[T])(implicit m: Manifest[T]): Unit = this.add(data, classOf[RDD[_]], m.erasure)

  private def forAllReps[T](f: RDD[_] ⇒ Any) {
    mReps.foreach {
      kv ⇒ if (kv._2 != null) f(kv._2.asInstanceOf[RDD[_]])
    }
  }

  override def cacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          mLog.info(this.getClass() + ": Persisting " + rdd)
          rdd.persist
        }
    })
  }

  override def uncacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          mLog.info(this.getClass() + ": Unpersisting " + rdd)
          rdd.unpersist(false)
        }
    })
  }
}

object RepresentationHandler {

  /**
   * Supported Representations
   */
  val RDD_ARRAY_DOUBLE = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[Double]]))
  val RDD_ARRAY_OBJECT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[Object]]))
  val RDD_LABELED_POINT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[LabeledPoint]))
  val RDD_TABLE_PARTITION = RH.getKeyFor(Array(classOf[RDD[_]], classOf[TablePartition]))
  val RDD_REXP = RH.getKeyFor(Array(classOf[RDD[_]], classOf[REXP]))
  /**
   *
   */
  def rowsToArraysObject(rdd: RDD[Row]): RDD[Array[Object]] = {
    rdd.map {
      row ⇒ row.rawdata.asInstanceOf[Array[Object]]
    }
  }

  def rowsToArraysDouble(rdd: RDD[Row], mappers: Array[Object ⇒ Double]): RDD[Array[Double]] = {
    val numCols = mappers.length
    rdd.map {
      row ⇒ rowToArray(row, classOf[Double], new Array[Double](numCols), mappers)
    }
  }

  def rowsToLabeledPoints(rdd: RDD[Row], mappers: Array[Object ⇒ Double]): RDD[LabeledPoint] = {
    val numCols = mappers.length
    rdd.map(row ⇒ {
      val features = rowToArray(row, classOf[Double], new Array[Double](numCols - 1), mappers)
      val label = mappers(numCols - 1)(row.getPrimitive(numCols - 1))
      new LabeledPoint(label, features)
    })
  }

  def rowToArray[T](row: Row, columnClass: Class[_], array: Array[T], mappers: Array[Object ⇒ T]): Array[T] = {
    var i = 0
    while (i < array.size) {
      array(i) = mappers(i)(row.getPrimitive(i))
      i += 1
    }
    array
  }

  def rowsToTablePartitions(rdd: RDD[Row]): RDD[TablePartition] = {
    rdd.map {
      row ⇒ row.rawdata.asInstanceOf[TablePartition]
    }
  }

  private def getDoubleMapper(colType: ColumnType): Object ⇒ Double = {
    colType match {
      case ColumnType.DOUBLE ⇒ {
        case obj ⇒ obj.asInstanceOf[Double]
      }

      case ColumnType.INT ⇒ {
        case obj ⇒ obj.asInstanceOf[Int].toDouble
      }

      case ColumnType.STRING ⇒ {
        case _ ⇒ throw new DDFException("Cannot convert string to double")
      }
    }
  }

  def rowsToNativeTable(ddf: DDF, rdd: RDD[Row], numCols: Int): NativeTable = null // TODO

  /**
   * Transform a bigr data frame into a rdd of native renjin dataframes per partition (as Java objects)
   * note that a R data frame is just a list of column vectors with additional attributes
   */
  def tablePartitionsToRDataFrame(rdd: RDD[TablePartition], columnList: java.util.List[Column]): RDD[REXP] = {
    rdd.filter { tp ⇒ tp.iterator.columnIterators.length > 0 }.map { tp ⇒
      println("tp.numRows = " + tp.numRows)
      println("columnIterators.length = " + tp.iterator.columnIterators.length)

      // each TablePartition should not have more than MAX_INT rows,
      // ArrayBuffer doesn't allow more than that anyway
      val numRows = tp.numRows.asInstanceOf[Int]

      val columns = columnList.zipWithIndex.map {
        case (colMeta, colNo) ⇒
          //mLog.info("processing column: {}, index = {}", colMeta, colNo)
          val iter = tp.iterator.columnIterators(colNo)
          val rvec = colMeta.getType match {
            case ColumnType.INT ⇒ {
              val builder = new mutable.ArrayBuilder.ofInt
              var i = 0
              while (i < tp.numRows) {
                iter.next()
                if (iter.current != null)
                  builder += iter.current.asInstanceOf[IntWritable].get
                else
                  builder += REXPInteger.NA
                i += 1
              }
              new REXPInteger(builder.result)
            }
            case ColumnType.DOUBLE ⇒ {
              val builder = new mutable.ArrayBuilder.ofDouble
              var i = 0
              while (i < tp.numRows) {
                iter.next()
                if (iter.current != null)
                  builder += iter.current.asInstanceOf[DoubleWritable].get
                else
                  builder += REXPDouble.NA
                i += 1
              }
              new REXPDouble(builder.result)
            }
            case ColumnType.STRING ⇒ {
              val buffer = new mutable.ArrayBuffer[String](numRows)
              var i = 0
              while (i < tp.numRows) {
                iter.next()
                if (iter.current != null)
                  buffer += iter.current.asInstanceOf[Text].toString
                else
                  buffer += null
                i += 1
              }
              new REXPString(buffer.toArray)
            }
            // TODO: REXPLogical
          }
          rvec.asInstanceOf[REXP]
      }

      // named list of columns with colnames
      val dflist = new RList(columns, columnList.map { m ⇒ m.getName })

      // this is the per-partition Renjin data.frame
      REXP.createDataFrame(dflist)
    }
  }
}