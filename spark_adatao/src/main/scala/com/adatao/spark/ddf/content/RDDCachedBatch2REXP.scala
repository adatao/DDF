package com.adatao.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.columnar.{NativeColumnAccessor, ColumnAccessor, CachedBatch}
import java.nio.ByteBuffer
import io.ddf.content.Schema.{ColumnType, Column}
import org.rosuda.REngine._
import com.adatao.spark.ddf.etl.TransformDummy
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import io.spark.ddf.content.{RepresentationHandler => SparkRepresentationHandler}

/**
 * author: daoduchuan
 */
class RDDCachedBatch2REXP(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val columnList = ddf.getSchemaHandler.getColumns
    representation.getValue match {
      case rdd: RDD[CachedBatch] => {

        val rddREXP = rdd.flatMap {
          cachedBatch => RDDCachedBatch2REXP.arrByteBuffer2REXP(cachedBatch, columnList)
        }
        new Representation(rddREXP, SparkRepresentationHandler.RDD_REXP.getTypeSpecsString)
      }
    }
  }
}

object RDDCachedBatch2REXP {
  val DEFAULT_NUM_RSERVER_SPLITS = "4"
  def arrByteBuffer2REXP(cachedBatch: CachedBatch, columns: java.util.List[Column]): Array[REXP] = {
    val arr = cachedBatch.buffers.map {
      arrByte => ByteBuffer.wrap(arrByte)
    }
    val numRows = cachedBatch.stats.getInt(3)
    val numSplits = System.getProperty("pa.Rserve.split", DEFAULT_NUM_RSERVER_SPLITS).toInt
    val numRowsPerSplit = numRows / numSplits + 1
    val REXPColumns = columns.zipWithIndex.map {
      case (column, idx) => {
        val columnAccessor = ColumnAccessor(arr(idx)).asInstanceOf[NativeColumnAccessor[_]]
        var currentSplit = 0
        var rowNumber = 0

        val arrOfVector = column.getType match {
          case ColumnType.INT => {
            val REXPBuffer = ArrayBuffer[REXP]()
            while(currentSplit < numSplits) {
              val builder = new mutable.ArrayBuilder.ofInt
              val mutableRow = new GenericMutableRow(1)
              var i = 0
              while((i < numRowsPerSplit) && (rowNumber < numRows)) {

                columnAccessor.extractSingle(mutableRow, 0)
                if(!mutableRow.isNullAt(0)) {
                  builder += mutableRow.getInt(0)
                } else {
                  builder += REXPInteger.NA
                }

                i += 1
                rowNumber += 1
              }
              REXPBuffer += (new REXPInteger(builder.result())).asInstanceOf[REXP]
              currentSplit += 1
            }
            REXPBuffer
          }
          case ColumnType.DOUBLE => {
            val REXPBuffer = ArrayBuffer[REXP]()
            while(currentSplit < numSplits) {
              val builder = new mutable.ArrayBuilder.ofDouble
              val mutableRow = new GenericMutableRow(1)
              var i = 0
              while((i < numRowsPerSplit) && (rowNumber < numRows)) {

                columnAccessor.extractSingle(mutableRow, 0)
                if(!mutableRow.isNullAt(0)) {
                  builder += mutableRow.getDouble(0)
                } else {
                  builder += REXPDouble.NA
                }

                i += 1
                rowNumber += 1
              }
              REXPBuffer += (new REXPDouble(builder.result())).asInstanceOf[REXP]
              currentSplit += 1
            }
            REXPBuffer
          }

          case ColumnType.STRING => {
            val REXPBuffer = ArrayBuffer[REXP]()
            while(currentSplit < numSplits) {
              val builder = ArrayBuffer[String]()
              val mutableRow = new GenericMutableRow(1)
              var i = 0
              while((i < numRowsPerSplit) && (rowNumber < numRows)) {

                columnAccessor.extractSingle(mutableRow, 0)
                if(!mutableRow.isNullAt(0)) {
                  builder += mutableRow.getString(0)
                } else {
                  builder += null
                }

                i += 1
                rowNumber += 1
              }
              REXPBuffer += (new REXPString(builder.toArray)).asInstanceOf[REXP]
              currentSplit += 1
            }
            REXPBuffer
          }
        }
        arrOfVector
      }
    }
    val listREXP: Array[ArrayBuffer[REXP]] = Array.fill(numSplits)(ArrayBuffer[REXP]())
    REXPColumns.foreach {
      splits => {
        var splitId = 0
        while(splitId < splits.size) {
          listREXP(splitId) += splits(splitId)
          splitId += 1
        }
      }
    }
    listREXP.map(partdf => new RList(partdf.toArray, columns.map(col => col.getName).toArray)).map(dfList => REXP.createDataFrame(dfList))
  }
}
