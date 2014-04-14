/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.Schema.ColumnType
import java.lang.Class
import scala.reflect.Manifest
import org.apache.spark.rdd.RDD
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
import com.adatao.ddf.types.Matrix
import com.adatao.ddf._
import com.adatao.ddf.types.Vector
import com.adatao.ddf.types._
import java.util.ArrayList
import java.util.HashMap
import com.adatao.spark.ddf.ml.TransformRow
import com.adatao.ddf.content.AMetaDataHandler.ICustomMetaData
import com.adatao.spark.ddf.content.MetaDataHandler.DummyCustomMetaData

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

    val metaHandler: MetaDataHandler = mDDF.getMetaDataHandler().asInstanceOf[MetaDataHandler]
    metaHandler.buildListCustomMetaData()
    val customMetaData = metaHandler.getListCustomMetaData()
    if (customMetaData != null) {
      println(">>>>>>>>>>>>>>>>>>>>>>>> customMetaData before call rowToMatrixVector " + customMetaData)
      println(">>>>>>>>>>>>>>>>>>>>>>>> metaHandler before call rowToMatrixVector " + metaHandler)
    }

    typeSpecs match {
      case RDD_TABLE_PARTITION ⇒ rowsToTablePartitions(srcRdd)
      case RDD_ARRAY_OBJECT ⇒ rowsToArraysObject(srcRdd)
      case RDD_ARRAY_DOUBLE ⇒ rowsToArraysDouble(srcRdd, mappers)
      case RDD_LABELED_POINT ⇒ rowsToLabeledPoints(srcRdd, mappers)
      case RDD_ARRAY_LABELED_POINT ⇒ rowsToArrayLabeledPoints(srcRdd, mappers)
      case RH.NATIVE_TABLE ⇒ rowsToNativeTable(mDDF, srcRdd, numCols)
      case RDD_MATRIX_VECTOR ⇒ {

        println(">>>>>>>>>>>>calling rowsToMatrixVector")
        rowsToMatrixVector(srcRdd, mappers, metaHandler)
      }
      case _ ⇒ throw new DDFException(String.format("TypeSpecs %s not supported. It must be one of:\n - %s\n - %s\n - %s\n - %s\n -%s",
        typeSpecs,
        RDD_TABLE_PARTITION, RDD_ARRAY_OBJECT, RDD_ARRAY_DOUBLE, RDD_LABELED_POINT, RH.NATIVE_TABLE))
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
    //    LabeledPoint
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
  val RDD_MATRIX_VECTOR = RH.getKeyFor(Array(classOf[RDD[_]], classOf[TupleMatrixVector]))
  val RDD_ARRAY_LABELED_POINT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[LabeledPoint]]))
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

  def rowsToArrayLabeledPoints(rdd: RDD[Row], mappers: Array[Object ⇒ Double]): RDD[Array[LabeledPoint]] = {
    val numCols = mappers.length
    rdd.mapPartitions(rows ⇒ {
      val numCols = mappers.length
      var lstRows = new ArrayList[LabeledPoint]()
      var row = 0
      while (rows.hasNext) {
        var currentRow = rows.next
        val features = rowToArray(currentRow, classOf[Double], new Array[Double](numCols - 1), mappers)
        val label = mappers(numCols - 1)(currentRow.getPrimitive(numCols - 1))
        lstRows.add(row, new LabeledPoint(label, features))
        row += 1
      }

      val numRows = lstRows.size //rows.toArray[Row].length
      val ret = new Array[LabeledPoint](numRows)
      row = 0
      while (row < lstRows.size) {
        ret(row) = lstRows(row)
        row += 1
      }
      Iterator(ret)
    })
  }

  //TODO move this method
  def rowsToArrayLabeledPoints2(rdd: RDD[Array[Double]]): RDD[Array[LabeledPoint]] = {
    rdd.mapPartitions(rows ⇒ {
      val numCols = 2
      var lstRows = new ArrayList[LabeledPoint]()
      var row = 0
      while (rows.hasNext) {
        var currentRow = rows.next
        val label = currentRow(0)
        val prediction: Array[Double] = new Array(1) // rowToArray(currentRow, classOf[Double], new Array[Double](numCols - 1), mappers)
        prediction(0) = currentRow(1)

        lstRows.add(row, new LabeledPoint(label, prediction))
        row += 1
      }

      val numRows = lstRows.size //rows.toArray[Row].length
      val ret = new Array[LabeledPoint](numRows)
      row = 0
      while (row < lstRows.size) {
        ret(row) = lstRows(row)
        row += 1
      }
      Iterator(ret)
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

  def rowsToMatrixVector(rdd: RDD[Row], mappers: Array[Object ⇒ Double], metaHandler: MetaDataHandler): RDD[TupleMatrixVector] = {

    //initialize xCols, in Representation Handler, we are asuming xCol have length = mapper.length -1 ALSO xCols[0] = 0, xCols[1] = 1 and so on
    //TODO initialize xCols
    var xCols: Array[Int] = new Array[Int](mappers.length - 1)

    println(">>>>> before calling printMetaData")
    
     
    printMetaData(metaHandler)

    //send to slave
    rdd.mapPartitions(rows => rowsToMatrixVector(rows, mappers, metaHandler))
  }

  def printMetaData(metaHandler: MetaDataHandler) {

    println(">>>>> calling printMetaData")
    
    if (metaHandler == null) {
      println(">>>>>>>>>>>>>>> printMetaData metaHandler is null")
    } else {
      println(">>>>>>>>>>>>>>> printMetaData metaHandler is NOT null = " + metaHandler)
    }

    val customMetaData = metaHandler.getListCustomMetaData()
    if (customMetaData == null) {
      println(">>>>>>>>>>>>>>> printMetaData customMetaData is null")
    } else {
      var it = customMetaData.keySet().iterator()
      println(">>>>>>>>>>>>>>> printMetaData iterating customMetaData ")
      while (it.hasNext()) {
        print(customMetaData.get(it.next()) + "\t")
      }
    }
  }

  def rowsToMatrixVector(rows: Iterator[Row], mappers: Array[Object ⇒ Double], metaHandler: MetaDataHandler): Iterator[TupleMatrixVector] = {

    val customMetaData = metaHandler.getListCustomMetaData()

    //number of original columns, including Y-column
    val numCols = mappers.length

    //copy original data to arrayList
    var lstRows = new ArrayList[Array[Double]]()
    var row = 0
    while (rows.hasNext) {
      var currentRow = rows.next
      var column = 0
      var b = new Array[Double](numCols)
      while (column < numCols) {
        b(column) = mappers(column)(currentRow.getPrimitive(column))
        column += 1
      }
      lstRows.add(row, b)
      row += 1
    }

    val numRows = lstRows.size
    val Y = new Vector(numRows)
    //    val X = new Matrix(numRows, numCols + trRow.numDummyCols)

    //TODO fix this
    var numDummyColumns = 0
    if (customMetaData != null)
      numDummyColumns = customMetaData.size

    val X = new Matrix(numRows, numCols + numDummyColumns)

    row = 0
    val yCol = 0

    while (row < lstRows.size) {
      var inputRow = lstRows(row)
      X.put(row, 0, 1.0) // bias term
      var columnValue = 0.0
      var newValue: Double = -1.0

      val paddingBiasIndex = 1

      var columnIndex = 0
      while (columnIndex < numCols - 1) {
        columnValue = inputRow(columnIndex)

        if (customMetaData != null && customMetaData.size > 0 && customMetaData.containsKey(columnIndex)) {
          //          println(">>>>>>>>>>>>>... customMetaData is not null: " + customMetaData)
          //           val currentCustomMetaData: DummyCustomMetaData = customMetaData.get(columnIndex).asInstanceOf[DummyCustomMetaData]
          val currentCustomMetaData = customMetaData.get(columnIndex)
          //          newValue = trRow.transform(columnIndex, columnValue + "")
          newValue = currentCustomMetaData.getColumnIndex(columnValue + "")
        }
        if (newValue == -1.0)
          newValue = columnValue

        X.put(row, columnIndex + paddingBiasIndex, newValue) // x-feature #i
        columnIndex += 1
        newValue = -1.0 // TODO: dirty and quick fix, need proper review
      }
      Y.put(row, inputRow(numCols - 1)) // y-value
      row += 1
    }
    val Z: TupleMatrixVector = new TupleMatrixVector(X, Y)
    Iterator(Z)
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
}