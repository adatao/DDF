/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark

import java.util.BitSet
import java.nio.ByteOrder
import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import shark.memstore2.TablePartition
import shark.memstore2.column._
import com.adatao.ML.TCanLog
import java.util


/**
 * This object extends the functionality of java DataManager.SharkDataFrame
 * @author bachbui
 */
object SharkDataFrameScala extends TCanLog {

	def getDataTable(rdd: RDD[TablePartition],
									 xCols: Array[Int],
									 yCol: Int,
									 categoricalMap: java.util.Map[java.lang.Integer, java.util.HashMap[String, java.lang.Double]] = null): RDD[(Matrix, Vector)] = {
		rdd.map(tablePartitionToMatrixVectorMapper(xCols, yCol, categoricalMap))
				.filter(xy ⇒ (xy._1.columns > 0) && (xy._2.rows > 0))
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
					for(i <- 0 until nullCount){
						val idx = buffer.getInt
						 nullBitmap.set(idx)
					}
				}
				case ci: ColumnIterator => LOG.warn(">>>>>>>>> got nonnullable coliter: " + ci.toString + ", class = " + ci.getClass)
			}
		)
		nullBitmap
	}

	def fillConstantColumn[T <: DoubleMatrix](matrix: T, col: Int, numRows: Int, value: Double) = {
		var row = 0
		while (row < numRows) {
			matrix.put(row, col, value)
			row += 1
		}
	}

	// for ColumnIterator that supports direct currentDouble without conversion
	def fillNumericColumn[M <: DoubleMatrix](
				matrix: M,
				col: Int,
				colIter: ColumnIterator,
				numRows: Int,
				nullBitmap: BitSet) = {
		var i = 0  // current matrix row counter
		var j = 0  // current ColumnIterator row counter
		while (i < numRows) {
			colIter.nextDouble()
			if (!nullBitmap.get(j)) {
				// here, the tablePartition has non-null values in all other columns being extracted
				matrix.put(i, col, colIter.currentDouble)
				i += 1
			}
			j += 1
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
		var i = 0  // current matrix row counter
		var j = 0  // current ColumnIterator row counter
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

	def tablePartitionToMatrixVectorMapper(xCols: Array[Int],
																				 yCol: Int,
																				 categoricalMap: java.util.Map[java.lang.Integer, java.util.HashMap[java.lang.String, java.lang.Double]])(tp: TablePartition): (Matrix, Vector) = {
		// get the list of used columns
		val xyCol = xCols :+ yCol

		if ( tp.iterator.columnIterators.isEmpty ) {

			(new Matrix(0, 0), Vector(0))

		} else {

			val usedColumnIterators: Array[ColumnIterator] = xyCol.map { colId ⇒ tp.iterator.columnIterators(colId) }.toArray

			//TODO: handle number of rows in long
			val nullBitmap = buildNullBitmap(tp.numRows.toInt, usedColumnIterators)
			val numRows = tp.numRows.toInt - nullBitmap.cardinality()
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
			val colIter = usedColumnIterators.last
			fillNumericColumn(Y, 0, colIter, numRows, nullBitmap) // TODO: has caller checked that yCol is numeric?

			// fill the rest of X, column by column (except for the dummy columns, which is filled at a later pass)
			var i = 1 // column index in X matrix
			while (i < numXCols) {
				val colIter = usedColumnIterators(i - 1)
				val xColId = xCols(i - 1)

				// The first 4 bytes after null encoding indicates the column type
				// null encoding: first 4 byte is null count, then null positions
				val buffer = colIter.asInstanceOf[NullableColumnIterator].getBuffer.duplicate().order(ByteOrder.nativeOrder())
				buffer.rewind()
				val nullCount = buffer.getInt
				buffer.position(buffer.position + nullCount * 4)
				val columnType: ColumnType[_, _] = Implicits.intToColumnType(buffer.getInt)

				// TODO: patch Shark to have a type-safe way of doing this, sth like trait DoubleableColumnIterator
				columnType match {
					case INT | LONG | FLOAT | DOUBLE | BOOLEAN | BYTE| SHORT => {
						LOG.info("extracting numeric column id {}, columnType {}", xColId, columnType.toString)

						if ( categoricalMap != null && categoricalMap.contains(xColId) ) {
							val columnMap = categoricalMap.get(xColId)
							LOG.info("extracting STRING column id {} using mapping {}", xColId, columnMap)

							fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
								// invariant: columnMap.contains(x)
								val k = current.toString
								columnMap.get(k)
							})

						} else {
							fillNumericColumn(X, i, colIter, numRows, nullBitmap)
						}
					}
					case STRING => {
						if ( categoricalMap != null && categoricalMap.contains(xColId) ) {
							val columnMap = categoricalMap.get(xColId)
							LOG.info("extracting STRING column id {} using mapping {}", xColId, columnMap)

							fillColumnWithConversion(X, i, colIter, numRows, nullBitmap, (current: Object) => {
								// invariant: columnMap.contains(x)
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

//			println("X = " + util.Arrays.deepToString(X.toArray2.asInstanceOf[Array[Object]]))
//			println("Y = " + util.Arrays.deepToString(Y.toArray2.asInstanceOf[Array[Object]]))

			(X, Y)
		}
	}
}
