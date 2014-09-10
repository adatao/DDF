package com.adatao.spark.ddf.etl

import io.ddf.DDF
import io.spark.ddf.etl.{ TransformationHandler => THandler }
import shark.memstore2.TablePartition
import io.spark.ddf.SparkDDF
import org.apache.spark.rdd.RDD
import java.util
import java.util.{ Map => JMap }
import io.ddf.types.Matrix
import io.ddf.types.Vector
import shark.memstore2.column.ColumnIterator
import shark.memstore2.column._
import java.util.BitSet
import shark.memstore2.column.NullableColumnIterator
import org.jblas.DoubleMatrix
import org.apache.hadoop.io.IntWritable
import shark.memstore2.column.ColumnType
import org.apache.hadoop.io._
import java.nio.ByteOrder
import java.util.HashMap
import io.spark.ddf.ml.TransformRow
import io.ddf.content.Schema
import io.ddf.types.TupleMatrixVector

/**
 */
class TransformationHandler(mDDF: DDF) extends THandler(mDDF) {

  def dummyCoding(xCols: Array[String], yCol: String): SparkDDF = {

    println(">>>>>>>>>>>>>> before dummy coding")

    mDDF.getSchemaHandler.setFactorLevelsForStringColumns(xCols)

    println(">>>>>>>>>>>>>> 11111111111")

    mDDF.getSchemaHandler.computeFactorLevelsAndLevelCounts()

    println(">>>>>>>>>>>>>> 22222222222")

    mDDF.getSchemaHandler.generateDummyCoding()

    println(">>>>>>>>>>>>>> after dummy coding")

    //convert column name to column index
    val xColsIndex: Array[Int] = xCols.map(columnName => mDDF.getSchema().getColumnIndex(columnName))
    val yColIndex: Int = mDDF.getSchema().getColumnIndex(yCol)
    val categoricalMap = mDDF.getSchema.getDummyCoding.getMapping()

    val tp = mDDF.asInstanceOf[SparkDDF].getRDD(classOf[TablePartition])
    //return Matrix Vector
    val mv = TransformDummy.getDataTable(tp, xColsIndex, yColIndex, categoricalMap)

    //check if contains dummy coding
    var hasDummyCoding = false
    var i = 0
    while (i < xColsIndex.length) {
      if (categoricalMap.containsKey(xColsIndex(i)))
        hasDummyCoding = true
      i += 1
    }

    //copy schema
    //convert to dummy column
    if (hasDummyCoding) {
      val mv2 = mv.map(TransformDummy.instrument(xColsIndex, categoricalMap))
      //build schema for dummyCodingDDF
      var cList = ""
      i = 0
      while (i < xCols.length) {
        var c = mDDF.getSchema.getColumn(xCols(i))
        //dummy columns
        if (c.getColumnClass() == Schema.ColumnClass.FACTOR && categoricalMap.containsKey(xColsIndex(i))) {
          //build map
          val currentMap = categoricalMap.get(xColsIndex(i))
          val it = currentMap.keySet().iterator()
          var dummyColumnsLabel = new Array[String](currentMap.size) 
          while(it.hasNext()) {
            val key = it.next()
            val value = Math.floor(currentMap.get(key)).intValue()
            dummyColumnsLabel(value) = c.getName() + "_" + key + " " + c.getType().toString().toLowerCase() + ","
          }
          //loop
          var j = 0
          while(j < currentMap.size) {
             cList += dummyColumnsLabel(j)
             j += 1
          }
          i += 1
        }
        else {
          cList += c.getName() + " " + c.getType().toString().toLowerCase() + ","
          i += 1
        }
      }
      cList += mDDF.getSchema.getColumn(yCol).getName() + " " + mDDF.getSchema.getColumn(yCol).getType().toString().toLowerCase()
      
      var schema = new Schema(mDDF.getSchema().getTableName() + "_dummy_" + yCol.toString(), cList);
      var dummyCodingDDF = new SparkDDF(mDDF.getManager(), mv2, classOf[TupleMatrixVector], mDDF.getNamespace(), mDDF.getNamespace(), schema)
      dummyCodingDDF
    } //no dummy coding
    else {

      //build schema for dummyCodingDDF
      var cList = ""
      i = 0
      while (i < xCols.length) {
        var c = mDDF.getSchema.getColumn(xCols(i))
        cList += c.getName() + " " + c.getType().toString().toLowerCase() + ","
        i += 1
      }
      cList += mDDF.getSchema.getColumn(yCol).getName() + " " + mDDF.getSchema.getColumn(yCol).getType().toString().toLowerCase()


      var schema = new Schema(mDDF.getSchema().getTableName() + "_dummy_" + yCol.toString(), cList);
      var dummyCodingDDF = new SparkDDF(mDDF.getManager(), mv, classOf[TupleMatrixVector], mDDF.getNamespace(), mDDF.getNamespace(), schema)
      dummyCodingDDF
    }
  }
}

object TransformDummy {
  def getDataTable(rdd: RDD[TablePartition],
    xCols: Array[Int],
    yCol: Int,
    categoricalMap: HashMap[Integer, HashMap[String, java.lang.Double]] = null): RDD[TupleMatrixVector] = {
    rdd.map(tablePartitionToMatrixVectorMapper(xCols, yCol, categoricalMap))
      .filter(xy ⇒ (xy._1.columns > 0) && (xy._2.rows > 0))
  }

  /* 
   * input: categoricalColumnSize is the mapping between column id and a number of unique values in categorical column 
   *    key = original column id of X
   *    value = length of dummy column, including original one 
   * input: original matrix
   * output: new matrix with new dummy columns
   */
  def instrument[InputType](xCols: Array[Int], mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]])(inputRow: TupleMatrixVector): TupleMatrixVector = {

    //so we need to do minus one for original column
    var oldX = inputRow._1
    var oldY = inputRow._2

    //add dummy columns
    val numCols = oldX.columns
    var numRows = oldX.rows
    var newColumnMap = new Array[Int](numCols)

    //row transformer
    var trRow = new TransformRow(xCols, mapping)

    //for each row
    var indexRow = 0
    var currentRow = null.asInstanceOf[Matrix]
    var newRowValues = null.asInstanceOf[DoubleMatrix]
    while (indexRow < oldX.rows) {

      //for each rows
      currentRow = Matrix(oldX.getRow(indexRow))
      newRowValues = trRow.transform(currentRow)
      //add new row
      oldX.putRow(indexRow, newRowValues)

      //convert oldX to new X
      indexRow += 1
    }
    new TupleMatrixVector(oldX, oldY)
  }

  def tablePartitionToMatrixVectorMapper(xCols: Array[Int], yCol: Int, categoricalMap: HashMap[Integer, HashMap[String, java.lang.Double]])(tp: TablePartition): TupleMatrixVector = {
    // get the list of used columns
    val xyCol = xCols :+ yCol

    if (tp.iterator.columnIterators.isEmpty) {

      (new TupleMatrixVector(new Matrix(0, 0), new Vector(0)))

    } else {

      val usedColumnIterators: Array[ColumnIterator] = xyCol.map { colId ⇒ tp.iterator.columnIterators(colId) }

      //TODO: handle number of rows in long
      val nullBitmap = buildNullBitmap(tp.numRows.toInt, usedColumnIterators)
      val numRows = tp.numRows.toInt - nullBitmap.cardinality()
      val numXCols = xCols.length + 1

      xCols(0)

      var numDummyCols = 0
      if (categoricalMap != null) {
        var i: Integer = 0
        while (i < xCols.length) {
          if (categoricalMap.containsKey(xCols(i))) {
            var currentMap = categoricalMap.get(xCols(i)).keySet()
            numDummyCols += currentMap.size() - 2
          }
          i += 1
        }
      }

      val Y = new Vector(numRows)
      val X = new Matrix(numRows, numXCols + numDummyCols) // this allocation won't be feasible for sparse features
      // fill in the first X column with bias value
      fillConstantColumn(X, 0, numRows, 1.0)

      // fill Y
      val colIter = usedColumnIterators.last
      fillNumericColumn(Y, 0, colIter, getColType(colIter), numRows, nullBitmap) // TODO: has caller checked that yCol is numeric?

      // fill the rest of X, column by column (except for the dummy columns, which is filled at a later pass)
      var i = 1 // column index in X matrix
      while (i < numXCols) {
        val colIter = usedColumnIterators(i - 1)
        val xColId = xCols(i - 1)
        val columnType = getColType(colIter)
        columnType match {
          case INT | LONG | FLOAT | DOUBLE | BOOLEAN | BYTE | SHORT => {
            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
              val columnMap = categoricalMap.get(xColId)
              fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
                // invariant: columnMap.contains(x)
                val k = current.toString
                columnMap.get(k)
              })

            } else {
              fillNumericColumn(X, i, colIter, columnType, numRows, nullBitmap)
            }
          }
          case STRING => {
            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
              val columnMap = categoricalMap.get(xColId)
              fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
                val k = current.asInstanceOf[Text].toString
                columnMap.get(k)
              })

            } else {
              throw new RuntimeException("got STRING column but no categorical map")
            }
          }
          case VOID | BINARY | TIMESTAMP | GENERIC => {
            throw new RuntimeException("don't know how to vectorize this column type: xColId = " + xColId + ", " + columnType.getClass.toString)
          }
        }

        i += 1
      }
      new TupleMatrixVector(X, Y)
    }
  }

  def buildNullBitmap(numRows: Int, usedColumnIterators: Array[ColumnIterator]): BitSet = {
    val nullBitmap: BitSet = new BitSet(numRows)

    usedColumnIterators.foreach(ci =>
      ci match {
        case nci: NullableColumnIterator => {
          // read from beginning of ByteBuffer to get null position
          val buffer = nci.getBuffer.duplicate().order(ByteOrder.nativeOrder())
          buffer.rewind()
          val nullCount = buffer.getInt
          for (i <- 0 until nullCount) {
            val idx = buffer.getInt
            nullBitmap.set(idx)
          }
        }
        case ci: ColumnIterator => System.err.println(">>>>>>>>> got nonnullable coliter: " + ci.toString + ", class = " + ci.getClass)
      })
    nullBitmap
  }

  def fillConstantColumn[T <: DoubleMatrix](matrix: T, col: Int, numRows: Int, value: Double) = {
    var row = 0
    while (row < numRows) {
      matrix.put(row, col, value)
      row += 1
    }
  }

  // for ColumnIterator that returns Object and must be converted to Double
  def fillColumnWithConversion[M <: DoubleMatrix](
    matrix: M,
    col: Int,
    colIter: ColumnIterator,
    numRows: Int,
    nullBitmap: BitSet,
    convert: (Object) => Double) = {
    var i = 0 // current matrix row counter
    var j = 0 // current ColumnIterator row counter
    while (i < numRows) {
      colIter.next()
      if (!nullBitmap.get(j)) {
        // here, the tablePartition has non-null values in all other columns being extracted
        matrix.put(i, col, convert(colIter.current))
        i += 1
      }
      j += 1
    }
  }

  // for ColumnIterator that supports direct currentDouble without conversion
  def fillNumericColumn[M <: DoubleMatrix](
    matrix: M,
    col: Int,
    colIter: ColumnIterator,
    colType: ColumnType[_, _],
    numRows: Int,
    nullBitmap: BitSet) = {
    var i = 0 // current matrix row counter
    var j = 0 // current ColumnIterator row counter
    // For performance, branching outside the tight loop
    val toDouble: (Object => Double) = colType match {
      case INT => (x: Object) => x.asInstanceOf[IntWritable].get().toDouble
      case LONG => (x: Object) => x.asInstanceOf[LongWritable].get.toDouble
      case FLOAT => (x: Object) => x.asInstanceOf[FloatWritable].get().toDouble
      case DOUBLE => (x: Object) => x.asInstanceOf[org.apache.hadoop.hive.serde2.io.DoubleWritable].get()
      case BOOLEAN => (x: Object) => { if (x.asInstanceOf[BooleanWritable].get()) 1 else 0 }
      case BYTE => (x: Object) => x.asInstanceOf[ByteWritable].get().toDouble
      case SHORT => (x: Object) => x.asInstanceOf[ShortWritable].get().toDouble
      case _ => throw new IllegalArgumentException("cannot not convert column type " + colType + " to double.")
    }
    while (i < numRows) {
      colIter.next()
      if (!nullBitmap.get(j)) {
        // here, the tablePartition has non-null values in all other columns being extracted
        matrix.put(i, col, toDouble(colIter.current))
        i += 1
      }
      j += 1
    }
  }

  def getColType(colIter: ColumnIterator): ColumnType[_, _] = {
    // decode the ColumnIterator to get ColumnType
    // using implementations details from Shark ColumnIterator.scala and NullableColumnIterator.scala
    // The first 4 bytes after null encoding indicates the column type
    // null encoding: first 4 byte is null count, then null positions
    val buffer = colIter.asInstanceOf[NullableColumnIterator].getBuffer.duplicate().order(ByteOrder.nativeOrder())
    buffer.rewind()

    val nullCount = buffer.getInt
    buffer.position(buffer.position + nullCount * 4)
    Implicits.intToColumnType(buffer.getInt)
  }
}
