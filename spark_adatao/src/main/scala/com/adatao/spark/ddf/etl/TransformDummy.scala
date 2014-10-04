package com.adatao.spark.ddf.etl

import io.ddf.types.{Matrix, Vector}
import java.util.{Map => JMap, HashMap, BitSet}
import io.ddf.types.TupleMatrixVector
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import org.apache.hadoop.io._
import org.apache.hadoop.hive.serde2.io.{ShortWritable, DoubleWritable}
import java.nio.ByteOrder
import java.nio.ByteBuffer
import org.apache.spark.sql.columnar._
import java.util
import io.ddf.exception.DDFException
import org.slf4j.LoggerFactory
import io.spark.ddf.ml.TransformRow

/**
 * author: daoduchuan
 */
object TransformDummy {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def schemaRDD2MatrixVector(inMemoryRelation: InMemoryRelation, xCols: Array[Int], yCol: Int,
                             categoricalMap: HashMap[Integer,
                               HashMap[String, java.lang.Double]] = null): RDD[TupleMatrixVector] = {
    inMemoryRelation.cachedColumnBuffers.map {
      arrayByteBuffer => {
        tablePartitionToMatrixVectorMapper(xCols, yCol, categoricalMap)(arrayByteBuffer)
      }
    }
  }

  def getNrowFromColumnIterator(columnIterators: Array[ByteBuffer]): Int = {
//    columnAccessor match {
//      case nci: NativeColumnAccessor[_] => {
//        val byteBuffer = nci.underlyingBuffer.duplicate().order(ByteOrder.nativeOrder())
//        byteBuffer.rewind()
//        val newColumnAccessor = ColumnAccessor(byteBuffer).asInstanceOf[NativeColumnAccessor[_]]
//        var count = 0
//        while (newColumnAccessor.hasNext) {
//          newColumnAccessor.extractSingle(byteBuffer)
//          count += 1
//        }
//        count
//      }
    val counts = columnIterators.map {
      bytebuffer =>  val columnAccessor = ColumnAccessor(bytebuffer).asInstanceOf[NativeColumnAccessor[_]]
      val buffer = columnAccessor.buffer
      var count = 0
      var terminated = false
      while (columnAccessor.hasNext && !terminated) {
        try {
          val value = columnAccessor.extractSingle(buffer)
          println(">>> value = " + value)
          println(">>> count = " + count)
          count += 1
        } catch {
          case e: java.nio.BufferUnderflowException => terminated = true
        }
      }
      println(">>> final count = " + count)
      count
    }
    counts.min
  }

  def buildNullBitmap(usedColumnIterators: Array[ByteBuffer]): BitSet = {
    val nullBitmap: BitSet = new BitSet()
    //LOG.info(">>>>> numRows = " + numRows)

    usedColumnIterators.foreach {
      buffer => {
        val bytebuffer = buffer.duplicate().order(ByteOrder.nativeOrder())
        // read from beginning of ByteBuffer to get null position
        bytebuffer.rewind()
        bytebuffer.getInt()
        val nullCount = bytebuffer.getInt()
        LOG.info(">>>>> nullCount = " + nullCount)
        for (i <- 0 until nullCount) {
          val idx = bytebuffer.getInt
          nullBitmap.set(idx)
        }
      }
    }
    nullBitmap
  }

  def fillConstantColumn[T <: DoubleMatrix](matrix: T, col: Int, numRows: Int, value: Double) = {
    var row = 0
    while (row < numRows) {
      matrix.put(row, col, value)
      row += 1
    }
  }

  def fillNumericColumn[M <: DoubleMatrix](matrix: M,
                                           col: Int,
                                           columnIterator: ByteBuffer,
                                           numRows: Int,
                                           nullBitmap: BitSet) = {
    val columnAccessor = ColumnAccessor(columnIterator).asInstanceOf[NativeColumnAccessor[_]]
    val bytebuffer = columnAccessor.underlyingBuffer
    var i = 0 // current matrix row counter
    var j = 0 // current ColumnIterator row counter
    // For performance, branching outside the tight loop
    val toDouble: (Object => Double) = columnAccessor.columnType match {
        case INT => (x: Object) => x.asInstanceOf[Int].toDouble
        case LONG => (x: Object) => x.asInstanceOf[Long].toDouble
        case FLOAT => (x: Object) => x.asInstanceOf[Float].toDouble
        case DOUBLE => (x: Object) => x.asInstanceOf[Double]
        case BOOLEAN => (x: Object) => {
          if (x.asInstanceOf[BooleanWritable].get()) 1 else 0
        }
        case BYTE => (x: Object) => x.asInstanceOf[ByteWritable].get().toDouble
        case SHORT => (x: Object) => x.asInstanceOf[ShortWritable].get().toDouble
        case _ => throw new IllegalArgumentException(s"cannot not convert column type ${columnAccessor.columnType} to double.")
      }
    while (i < numRows) {
      if (!nullBitmap.get(j)) {
        // here, the tablePartition has non-null values in all other columns being extracted
        val value = columnAccessor.extractSingle(bytebuffer)
        println(">>>> fillNumericColumn: value = " + value)
        matrix.put(i, col, toDouble(value.asInstanceOf[Object]))
        i += 1
      }
      j += 1
    }
  }

  // for ColumnIterator that returns Object and must be converted to Double
  def fillColumnWithConversion[M <: DoubleMatrix](
                                                   matrix: M,
                                                   col: Int,
                                                   columnIterator: ByteBuffer,
                                                   numRows: Int,
                                                   nullBitmap: BitSet,
                                                   convert: (Object) => Double) = {
    //val byteBuffer = columnAccessor.buffer
    val columnAccessor = ColumnAccessor(columnIterator).asInstanceOf[NativeColumnAccessor[_]]
    val bytebuffer = columnAccessor.underlyingBuffer
    LOG.info(">>>> fillColumnWithConversion columnType = " + columnAccessor.columnType.toString())
    var i = 0 // current matrix row counter
    var j = 0 // current ColumnIterator row counter
    while (i < numRows) {

      if (!nullBitmap.get(j)) {
        matrix.put(i, col, convert(columnAccessor.extractSingle(bytebuffer).asInstanceOf[Object]))
        i += 1
      }
      j += 1
    }
  }

  def tablePartitionToMatrixVectorMapper(xCols: Array[Int],
                                         yCol: Int,
                                         categoricalMap: HashMap[java.lang.Integer,
                                           HashMap[java.lang.String, java.lang.Double]])
                                        (columnIterators: Array[ByteBuffer]): TupleMatrixVector = {
    // get the list of used columns
    val xyCol = xCols :+ yCol

    if (columnIterators.size == 0) {
      new TupleMatrixVector(new Matrix(0, 0), Vector(0))
    } else {
      val usedColumnIterators: Array[ByteBuffer] = xyCol.map {
        colId ⇒ columnIterators(colId)
      }
      val numElements = this.getNrowFromColumnIterator(usedColumnIterators)
      //TODO: handle number of rows in long
      val nullBitmap = buildNullBitmap(usedColumnIterators)
      val numRows = numElements //numElements - nullBitmap.cardinality()
      val numXCols = xCols.length + 1

      var numDummyCols = 0
      if (categoricalMap != null) {
        val iterator2 = categoricalMap.keySet().iterator()
        while (iterator2.hasNext) {
          numDummyCols += categoricalMap.get(iterator2.next).size() - 2
        }

      }

      val Y = new Vector(numRows)
      val X = new Matrix(numRows, numXCols + numDummyCols) // this allocation won't be feasible for sparse features

      LOG.info("tablePartitiontoMapper: numRows = {}, null bitmap cardinality = {}, xCols = {}, nunNewFeatures = {}",
        numRows.toString, nullBitmap.cardinality().toString, util.Arrays.toString(xCols), numDummyCols.toString)

      // fill in the first X column with bias value
      fillConstantColumn(X, 0, numRows, 1.0)

      // fill Y
      val yColumnIter = usedColumnIterators.last

      fillNumericColumn(Y, 0, yColumnIter, numRows, nullBitmap) // TODO: has caller checked that yCol is numeric?

      // fill the rest of X, column by column (except for the dummy columns, which is filled at a later pass)
      var i = 1 // column index in X matrix
      while (i < numXCols) {
        val columnIterator = usedColumnIterators(i - 1)
        val xColId = xCols(i - 1)
        val columnAccessor = ColumnAccessor(columnIterator).asInstanceOf[NativeColumnAccessor[_]]
        val columnType = columnAccessor.columnType

        columnType match {
          case INT | LONG | FLOAT | DOUBLE | BOOLEAN | BYTE | SHORT => {
            LOG.info("extracting numeric column id {}, columnType {}", xColId, columnType.toString)

            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
              val columnMap = categoricalMap.get(xColId)
              LOG.info(s">>>> columnMap = null??? ${columnMap == null}")
              LOG.info("extracting categorical column id {} using mapping {}", xColId, columnMap)

              fillColumnWithConversion(X, i, columnIterator, numRows, nullBitmap, (current: Object) => {
                // invariant: columnMap.contains(x)
                val k = current.toString
                columnMap.get(k)
              })

            } else {
              fillNumericColumn(X, i, columnIterator, numRows, nullBitmap)
            }
          }
          case STRING => {
            if (categoricalMap != null && categoricalMap.containsKey(xColId)) {
              val columnMap = categoricalMap.get(xColId)
              LOG.info("extracting STRING column id {} using mapping {}", xColId, columnMap)

              fillColumnWithConversion(X, i, columnIterator, numRows, nullBitmap, (current: Object) => {
                // invariant: columnMap.contains(x)
                val k = current.toString
                val value = columnMap.get(k)
                value
              })

            } else {
              throw new RuntimeException("got STRING column but no categorical map")
            }
          }
          case _ => {
            throw new RuntimeException("don't know how to vectorize this column type: xColId = " + xColId + ", " + columnType.getClass.toString)
          }
        }

        i += 1
      }
      new TupleMatrixVector(X, Y)
    }
  }

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
}
