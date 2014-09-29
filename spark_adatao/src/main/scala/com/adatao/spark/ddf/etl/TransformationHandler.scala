package com.adatao.spark.ddf.etl

import io.ddf.DDF
import io.spark.ddf.etl.{ TransformationHandler => THandler }
import io.spark.ddf.{SparkDDFManager, SparkDDF}
import org.apache.spark.rdd.RDD
import java.util
import java.util.{ Map => JMap }
import io.ddf.types.Matrix
import io.ddf.types.Vector

import java.util.BitSet
import org.jblas.DoubleMatrix
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io._
import java.nio.ByteOrder
import java.util.HashMap
import io.spark.ddf.ml.TransformRow
import io.ddf.content.Schema
import io.ddf.types.TupleMatrixVector
import scala.collection.JavaConversions._
import com.rits.cloning.Cloner
import io.ddf.etl.IHandleTransformations
import io.ddf.exception.DDFException
import org.apache.spark.sql.SchemaRDD
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.sql.columnar.InMemoryRelation

/**
 */
class TransformationHandler(mDDF: DDF) extends THandler(mDDF) {

  def dummyCoding(xCols: Array[String], yCol: String): SparkDDF = {

    mDDF.getSchemaHandler.setFactorLevelsForStringColumns(xCols)
    mDDF.getSchemaHandler.computeFactorLevelsAndLevelCounts()
    mDDF.getSchemaHandler.generateDummyCoding()

    //convert column name to column index
    val xColsIndex: Array[Int] = xCols.map(columnName => mDDF.getSchema().getColumnIndex(columnName))
    val yColIndex: Int = mDDF.getSchema().getColumnIndex(yCol)
    val categoricalMap = mDDF.getSchema.getDummyCoding.getMapping()

    //return Matrix Vector
    //TODO: migrate to SPARKSQL
    val hiveContext = mDDF.getManager.asInstanceOf[SparkDDFManager].getHiveContext
    val isCached = mDDF.asInstanceOf[SparkDDF].isCached
    mDDF.asInstanceOf[SparkDDF].cacheTable()
    val inMemoryRelation = hiveContext.table(mDDF.getTableName).queryExecution.analyzed match {
      case inMemory: InMemoryRelation => inMemory
      case something => throw new DDFException("Not InMemoryRelation, class = " + something.getClass.toString)
    }
    //mDDF.asInstanceOf[SparkDDF].unCacheTable()
    val rddMatrixVector: RDD[TupleMatrixVector] = TransformDummy.schemaRDD2MatrixVector(inMemoryRelation, xColsIndex, yColIndex, categoricalMap) //TransformDummy.getDataTable(tp, xColsIndex, yColIndex, categoricalMap)
    if(!isCached) {
      mDDF.asInstanceOf[SparkDDF].unCacheTable()
    }
    //check if contains dummy coding
    var hasDummyCoding = false
    var i = 0
    while (i < xColsIndex.length) {
      if (categoricalMap.containsKey(xColsIndex(i)))
        hasDummyCoding = true
      i += 1
    }
    val trainedColumns = (xCols :+ yCol).map{colIdx => mDDF.getSchema.getColumn(colIdx)}
    val newSchema = new Schema(null, trainedColumns)
    //copy schema
    //convert to dummy column
    if (hasDummyCoding) {
      val rddMatrixVector2: RDD[TupleMatrixVector] = rddMatrixVector.map(TransformDummy.instrument(xColsIndex, categoricalMap))
      val cloner = new Cloner()

      val dummyCoding = cloner.deepClone(mDDF.getSchema.getDummyCoding)
      val colNameMapping = dummyCoding.getMapping.map {
        case (k,v) => (mDDF.getSchema.getColumnName(k), v)
      }

      dummyCoding.setColNameMapping(colNameMapping)
      newSchema.setDummyCoding(dummyCoding)
      new SparkDDF(mDDF.getManager(), rddMatrixVector2, classOf[TupleMatrixVector], mDDF.getNamespace(), null, newSchema)
    } else {
      //build schema for dummyCodingDDF

      new SparkDDF(mDDF.getManager(), rddMatrixVector, classOf[TupleMatrixVector], mDDF.getNamespace(), null, newSchema)
    }
  }
}

object TransformationHandler {
  implicit def transformationHandlerConversion(tHandler: IHandleTransformations): TransformationHandler = {
    tHandler match {
      case trHandler: TransformationHandler => trHandler
      case x => throw new DDFException(String.format("Could not convert %s into com.adatao.spark.ddf.etl.TransformationHandler", x.getClass.toString))
    }
  }
}

//object TransformDummy {
////  def getDataTable(rdd: RDD[TablePartition],
////    xCols: Array[Int],
////    yCol: Int,
////    categoricalMap: HashMap[Integer, HashMap[String, java.lang.Double]] = null): RDD[TupleMatrixVector] = {
////    rdd.map(tablePartitionToMatrixVectorMapper(xCols, yCol, categoricalMap))
////      .filter(xy ⇒ (xy._1.columns > 0) && (xy._2.xx > 0))
////  }
////
////  /*
////   * input: categoricalColumnSize is the mapping between column id and a number of unique values in categorical column
////   *    key = original column id of X
////   *    value = length of dummy column, including original one
////   * input: original matrix
////   * output: new matrix with new dummy columns
////   */
//  def instrument[InputType](xCols: Array[Int], mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]])(inputRow: TupleMatrixVector): TupleMatrixVector = {
//
//    //so we need to do minus one for original column
//    var oldX = inputRow._1
//    var oldY = inputRow._2
//
//    //add dummy columns
//    val numCols = oldX.columns
//    var numRows = oldX.rows
//    var newColumnMap = new Array[Int](numCols)
//
//    //row transformer
//    var trRow = new TransformRow(xCols, mapping)
//
//    //for each row
//    var indexRow = 0
//    var currentRow = null.asInstanceOf[Matrix]
//    var newRowValues = null.asInstanceOf[DoubleMatrix]
//    while (indexRow < oldX.rows) {
//
//      //for each rows
//      currentRow = Matrix(oldX.getRow(indexRow))
//      newRowValues = trRow.transform(currentRow)
//      //add new row
//      oldX.putRow(indexRow, newRowValues)
//
//      //convert oldX to new X
//      indexRow += 1
//    }
//    new TupleMatrixVector(oldX, oldY)
//  }
////
////  def tablePartitionToMatrixVectorMapper(xCols: Array[Int], yCol: Int, categoricalMap: HashMap[Integer, HashMap[String, java.lang.Double]])(tp: TablePartition): TupleMatrixVector = {
////    // get the list of used columns
////    val xyCol = xCols :+ yCol
////
////    if (tp.iterator.columnIterators.isEmpty) {
////
////      (new TupleMatrixVector(new Matrix(0, 0), new Vector(0)))
////
////    } else {
////
////      val usedColumnIterators: Array[ColumnIterator] = xyCol.map { colId ⇒ tp.iterator.columnIterators(colId) }
////
////      //TODO: handle number of rows in long
////      val nullBitmap = buildNullBitmap(tp.numRows.toInt, usedColumnIterators)
////      val numRows = tp.numRows.toInt - nullBitmap.cardinality()
////      val numXCols = xCols.length + 1
////
////      xCols(0)
////
////      var numDummyCols = 0
////      if (categoricalMap != null) {
////        var i: Integer = 0
////        while (i < xCols.length) {
////          if (categoricalMap.containsKey(xCols(i))) {
////            var currentMap = categoricalMap.get(xCols(i)).keySet()
////            numDummyCols += currentMap.size() - 2
////          }
////          i += 1
////        }
////      }
////
////      val Y = new Vector(numRows)
////      val X = new Matrix(numRows, numXCols + numDummyCols) // this allocation won't be feasible for sparse features
////      // fill in the first X column with bias value
////      fillConstantColumn(X, 0, numRows, 1.0)
////
////      // fill Y
////      val colIter = usedColumnIterators.last
////      fillNumericColumn(Y, 0, colIter, getColType(colIter), numRows, nullBitmap) // TODO: has caller checked that yCol is numeric?
////
////      // fill the rest of X, column by column (except for the dummy columns, which is filled at a later pass)
////      var i = 1 // column index in X matrix
////      while (i < numXCols) {
////        val colIter = usedColumnIterators(i - 1)
////        val xColId = xCols(i - 1)
////        val columnType = getColType(colIter)
////        columnType match {
////          case INT | LONG | FLOAT | DOUBLE | BOOLEAN | BYTE | SHORT => {
////            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
////              val columnMap = categoricalMap.get(xColId)
////              fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
////                // invariant: columnMap.contains(x)
////                val k = current.toString
////                columnMap.get(k)
////              })
////
////            } else {
////              fillNumericColumn(X, i, colIter, columnType, numRows, nullBitmap)
////            }
////          }
////          case STRING => {
////            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
////              val columnMap = categoricalMap.get(xColId)
////              fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
////                val k = current.asInstanceOf[Text].toString
////                columnMap.get(k)
////              })
////
////            } else {
////              throw new RuntimeException("got STRING column but no categorical map")
////            }
////          }
////          case VOID | BINARY | TIMESTAMP | GENERIC => {
////            throw new RuntimeException("don't know how to vectorize this column type: xColId = " + xColId + ", " + columnType.getClass.toString)
////          }
////        }
////
////        i += 1
////      }
////      new TupleMatrixVector(X, Y)
////    }
////  }
////
////  def buildNullBitmap(numRows: Int, usedColumnIterators: Array[ColumnIterator]): BitSet = {
////    val nullBitmap: BitSet = new BitSet(numRows)
////
////    usedColumnIterators.foreach(ci =>
////      ci match {
////        case nci: NullableColumnIterator => {
////          // read from beginning of ByteBuffer to get null position
////          val buffer = nci.getBuffer.duplicate().order(ByteOrder.nativeOrder())
////          buffer.rewind()
////          val nullCount = buffer.getInt
////          for (i <- 0 until nullCount) {
////            val idx = buffer.getInt
////            nullBitmap.set(idx)
////          }
////        }
////        case ci: ColumnIterator => System.err.println(">>>>>>>>> got nonnullable coliter: " + ci.toString + ", class = " + ci.getClass)
////      })
////    nullBitmap
////  }
////
////  def fillConstantColumn[T <: DoubleMatrix](matrix: T, col: Int, numRows: Int, value: Double) = {
////    var row = 0
////    while (row < numRows) {
////      matrix.put(row, col, value)
////      row += 1
////    }
////  }
////
////  // for ColumnIterator that returns Object and must be converted to Double
////  def fillColumnWithConversion[M <: DoubleMatrix](
////    matrix: M,
////    col: Int,
////    colIter: ColumnIterator,
////    numRows: Int,
////    nullBitmap: BitSet,
////    convert: (Object) => Double) = {
////    var i = 0 // current matrix row counter
////    var j = 0 // current ColumnIterator row counter
////    while (i < numRows) {
////      colIter.next()
////      if (!nullBitmap.get(j)) {
////        // here, the tablePartition has non-null values in all other columns being extracted
////        matrix.put(i, col, convert(colIter.current))
////        i += 1
////      }
////      j += 1
////    }
////  }
////
////  // for ColumnIterator that supports direct currentDouble without conversion
////  def fillNumericColumn[M <: DoubleMatrix](
////    matrix: M,
////    col: Int,
////    colIter: ColumnIterator,
////    colType: ColumnType[_, _],
////    numRows: Int,
////    nullBitmap: BitSet) = {
////    var i = 0 // current matrix row counter
////    var j = 0 // current ColumnIterator row counter
////    // For performance, branching outside the tight loop
////    val toDouble: (Object => Double) = colType match {
////      case INT => (x: Object) => x.asInstanceOf[IntWritable].get().toDouble
////      case LONG => (x: Object) => x.asInstanceOf[LongWritable].get.toDouble
////      case FLOAT => (x: Object) => x.asInstanceOf[FloatWritable].get().toDouble
////      case DOUBLE => (x: Object) => x.asInstanceOf[org.apache.hadoop.hive.serde2.io.DoubleWritable].get()
////      case BOOLEAN => (x: Object) => { if (x.asInstanceOf[BooleanWritable].get()) 1 else 0 }
////      case BYTE => (x: Object) => x.asInstanceOf[ByteWritable].get().toDouble
////      case SHORT => (x: Object) => x.asInstanceOf[ShortWritable].get().toDouble
////      case _ => throw new IllegalArgumentException("cannot not convert column type " + colType + " to double.")
////    }
////    while (i < numRows) {
////      colIter.next()
////      if (!nullBitmap.get(j)) {
////        // here, the tablePartition has non-null values in all other columns being extracted
////        matrix.put(i, col, toDouble(colIter.current))
////        i += 1
////      }
////      j += 1
////    }
////  }
////
////  def getColType(colIter: ColumnIterator): ColumnType[_, _] = {
////    // decode the ColumnIterator to get ColumnType
////    // using implementations details from Shark ColumnIterator.scala and NullableColumnIterator.scala
////    // The first 4 bytes after null encoding indicates the column type
////    // null encoding: first 4 byte is null count, then null positions
////    val buffer = colIter.asInstanceOf[NullableColumnIterator].getBuffer.duplicate().order(ByteOrder.nativeOrder())
////    buffer.rewind()
////
////    val nullCount = buffer.getInt
////    buffer.position(buffer.position + nullCount * 4)
////    Implicits.intToColumnType(buffer.getInt)
////  }
//}
