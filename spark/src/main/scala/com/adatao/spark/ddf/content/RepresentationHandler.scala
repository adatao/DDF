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
import com.adatao.ddf.types.Matrix
import com.adatao.ddf._
import com.adatao.ddf.types.Vector
import com.adatao.ddf.types._
import org.jblas.DoubleMatrix
import java.util.ArrayList
import java.util.HashMap
import com.adatao.spark.ddf.ml.TransformRow
import com.adatao.ddf.content.AMetaDataHandler.ICustomMetaData
import com.adatao.spark.ddf.content.MetaDataHandler._
import com.adatao.ddf.content.Schema.ColumnClass
import com.adatao.ddf.content.Schema._
import com.adatao.ddf.content.Schema.DummyCoding

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

    mLog.info(">>>>>>> CREATING REPRESENTATION = " + typeSpecs)
    val schema = schemaHandler.getSchema()
    mLog.info(">>>>>>> typeSpecs = " + typeSpecs)
    typeSpecs match {
      case RDD_ROW => srcRdd
      case RDD_REXP ⇒ tablePartitionsToRDataFrame(mReps.get(RDD_TABLE_PARTITION).asInstanceOf[RDD[TablePartition]], schemaHandler.getColumns)
      case RDD_TABLE_PARTITION ⇒ rowsToTablePartitions(srcRdd)
      case RDD_ARRAY_OBJECT ⇒ rowsToArraysObject(srcRdd)
      case RDD_ARRAY_DOUBLE ⇒ rowsToArraysDouble(srcRdd, mappers)
      case RDD_LABELED_POINT ⇒ rowsToLabeledPoints(srcRdd, mappers)
      case RDD_ARRAY_LABELED_POINT ⇒ rowsToArrayLabeledPoints(srcRdd, mappers)
      case RH.NATIVE_TABLE ⇒ rowsToNativeTable(mDDF, srcRdd, numCols)
      case RDD_MATRIX_VECTOR ⇒ {

        //must invoke generate dummy coding explicitly, AGAIN
        schemaHandler.generateDummyCoding() //generateDummyCoding(schema)
        val dummyCoding = schemaHandler.getSchema().getDummyCoding()

        rowsToMatrixVectorRDD(srcRdd, mappers, dummyCoding)
      }
      case _ ⇒ throw new DDFException(String.format("TypeSpecs %s not supported. It must be one of:\n - %s\n - %s\n - %s\n - %s\n -%s\n - %s\n - %s",
        typeSpecs,
        RDD_ROW, RDD_REXP, RDD_TABLE_PARTITION, RDD_ARRAY_OBJECT, RDD_ARRAY_DOUBLE, RDD_LABELED_POINT, RH.NATIVE_TABLE))
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

    val arraysObject = this.get(classOf[RDD[_]], classOf[Array[Object]]).asInstanceOf[RDD[Array[Object]]]
    if (arraysObject != null) {
      return null // TODO
    }

    val arraysDouble = this.get(classOf[RDD[_]], classOf[Array[Double]]).asInstanceOf[RDD[Array[Double]]]
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
  val RDD_ROW = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Row]))
  val RDD_ARRAY_DOUBLE = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[Double]]))
  val RDD_ARRAY_OBJECT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[Object]]))
  val RDD_LABELED_POINT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[LabeledPoint]))
  val RDD_TABLE_PARTITION = RH.getKeyFor(Array(classOf[RDD[_]], classOf[TablePartition]))
  val RDD_REXP = RH.getKeyFor(Array(classOf[RDD[_]], classOf[REXP]))
  val RDD_MATRIX_VECTOR = RH.getKeyFor(Array(classOf[RDD[_]], classOf[TupleMatrixVector]))
  val RDD_ARRAY_LABELED_POINT = RH.getKeyFor(Array(classOf[RDD[_]], classOf[Array[LabeledPoint]]))
  /**
   *
   */
  def rowsToArraysObject(rdd: RDD[Row]): RDD[Array[Object]] = {
    rdd.map {
      row ⇒
        {
          val size = row.rawdata.asInstanceOf[Array[Object]].size
          val array = new Array[Object](size)
          var idx = 0
          while (idx < size) {
            array(idx) = row.getPrimitive(idx)
            idx += 1
          }
          array
        }
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

  def rowsToMatrixVectorRDD(rdd: RDD[Row], mappers: Array[Object ⇒ Double], dc: DummyCoding): RDD[TupleMatrixVector] = {

    //initialize xCols, in Representation Handler, we are asuming xCol have length = mapper.length -1 ALSO xCols[0] = 0, xCols[1] = 1 and so on
    //TODO initialize xCols
    println(">>>>> before calling printMetaData")

    //send to slave
    rdd.mapPartitions(rows => rowsToMatrixVector(rows, mappers, dc))
  }

  //  def printMetaData(dc: HashMap[Integer, HashMap[String, java.lang.Double]]) {
  //
  //    println(">>>>> calling printMetaData")
  //
  //    if (dc == null) {
  //      println(">>>>>>>>>>>>>>> printMetaData dummy coding is null")
  //    } else {
  //      println(">>>>>>>>>>>>>>> printMetaData dummy codingNOT null = " + dc)
  //    }
  //
  //  }

  def rowsToMatrixVector(rows: Iterator[Row], mappers: Array[Object ⇒ Double], dc: DummyCoding): Iterator[TupleMatrixVector] = {

    println(">>> inside rowsToMatrixVector dummy coding = " + dc)

    //number of original columns, including Y-column
    val numCols = mappers.length

    //copy original data to arrayList
    var lstRows = new ArrayList[Array[Object]]()
    var row = 0
    while (rows.hasNext) {
      var currentRow = rows.next
      var column = 0
      var b = new Array[Object](numCols)
      while (column < numCols) {
        //        b(column) = mappers(column)(currentRow.getPrimitive(column))
        b(column) = currentRow.getPrimitive(column)
        column += 1
      }
      lstRows.add(row, b)
      row += 1
    }

    val numRows = lstRows.size
    var Y = new Vector(numRows)
    //    val X = new Matrix(numRows, numCols + trRow.numDummyCols)
    println(">>>>>>>>>>>>>>>.. numDummyColumns= " + dc.getNumDummyCoding)

//    val X = new Matrix(numRows, numCols + dc.getNumDummyCoding)
    val X = new Matrix(numRows, dc.getNumberFeatures())
    //    var newX = new Matrix(numRows, numCols + numDummyColumns)
    //    val newY = new Vector(numRows)

    val trRow = new TransformRow(dc.xCols, dc.getMapping)

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

        var columnStringValue = ""
        //if this column is categorical column
        if (trRow.hasCategoricalColumn() && trRow.hasCategoricalColumn(columnIndex)) {
          columnStringValue = inputRow(columnIndex).toString() + ""
          newValue = dc.getMapping.get(columnIndex).get(columnStringValue)
          println("\tcolumnIndex=" + columnIndex + "\tcolumnStringValue=" + columnStringValue + "\tnewValue=" + newValue)
        }
        //normal numeric column
        if (newValue == -1.0)
          newValue = objectToDouble(inputRow(columnIndex))

        X.put(row, columnIndex + paddingBiasIndex, newValue) // x-feature #i

        columnIndex += 1
        newValue = -1.0 // TODO: dirty and quick fix, need proper review
      }
      Y.put(row, inputRow(numCols - 1).toString().toDouble) // y-value
      row += 1
    }

    println(">>>>>>>>>>>>> final OLD X matrix = " + X.toString())

    if (dc.getNumDummyCoding > 0) {
      //most important step
      var newX: Matrix = trRow.instrument(X, dc.getMapping, dc.getxCols)
      //let's print the matrix
      println(">>>>>>>>>>>>> NEWWWWWWWWWWWWWW final X matrix = " + newX.toString())
      val Z: TupleMatrixVector = new TupleMatrixVector(newX, Y)
      Iterator(Z)
    } else {
      val Z: TupleMatrixVector = new TupleMatrixVector(X, Y)
      Iterator(Z)
    }

  }

  def objectToDouble(o: Object): Double = o match {
    case i: java.lang.Integer => i.toDouble
    case f: java.lang.Float => f.toDouble
    case d: java.lang.Double => d
    case e => throw new RuntimeException("not a numeric Object " + (if (e != null) e.toString()))
  }

  def rowsToTablePartitions(rdd: RDD[Row]): RDD[TablePartition] = {
    rdd.map {
      row ⇒ row.rawdata.asInstanceOf[TablePartition]
    }
    rdd.asInstanceOf[RDD[TablePartition]]
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
