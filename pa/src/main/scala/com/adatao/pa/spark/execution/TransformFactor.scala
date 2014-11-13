package com.adatao.pa.spark.execution

import java.lang.{ Integer => JInt }
import java.lang.{Double => JDouble}
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import scala.collection.JavaConversions._
import io.ddf.content.Schema
import io.ddf.DDF
import io.ddf.content.Schema.Column
import java.util
import scala.util
import com.adatao.pa.spark.Utils.DataFrameResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * author: daoduchuan
 * Transform all factor column to index 0
 * example:
 * {A, B, C, D, A...} => {0, 1, 2, 3, 0, ...}
 * {1997, 2000, 1990, 1993, 2000} => {0, 1, 2, 3, 1, ...}
 */
class TransformFactor(dataContainerID: String) extends AExecutor[(DataFrameResult, Array[(JInt, java.util.Map[String, JDouble])])] {

  override def runImpl(ctx: ExecutionContext): (DataFrameResult, Array[(JInt, java.util.Map[String, JDouble])]) = {
    val ddf = ctx.sparkThread.getDDFManager.getDDF(dataContainerID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find ddf", null)
    }
    val factors = TransformFactor.getFactor(ddf, ctx)
    val keyValue:  Array[(JInt, java.util.Map[String, JDouble])] = factors.map{
      case (colIdx, hmap) => {
        val keys = hmap.keySet()
        var value = 0.0
        val keyValueMap: java.util.Map[String, JDouble] = new java.util.HashMap[String, JDouble]()
        for(key <- keys) {
          keyValueMap.put(key, value)
          value += 1.0
        }
        (colIdx, keyValueMap)
      }
    }
    val keyValueMap = keyValue.toMap
    LOG.info(">>> keyValueMap = " + keyValueMap.keySet.mkString(", "))
    val colFactorIndexes: List[Int] = keyValue.map{case (idx, hmap) => idx.toInt}.toList

    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val numCols = ddf.getNumColumns
    val colTypes = ddf.getSchemaHandler.getColumns.map{col => col.getType}
    val newRDD = rddRow.map{
      row => {
        var idx = 0
        val arr = Array[Double](numCols)
        while(idx < numCols) {
          val value = row.get(idx)
          if(colFactorIndexes.contains(idx)) {
            arr(idx) = keyValueMap(idx).get(value.toString)
          } else {
            arr(idx) = if(colTypes(idx) == Schema.ColumnType.INT) {
              row.getInt(idx).toDouble
            } else if(colTypes(idx) == Schema.ColumnType.DOUBLE) {
              row.getDouble(idx)
            } else if(colTypes(idx) == Schema.ColumnType.FLOAT) {
              row.getFloat(idx).toDouble
            } else if(colTypes(idx) == Schema.ColumnType.LONG) {
              row.getLong(idx).toDouble
            } else {
              0.0
            }
          }
          idx += 1
        }
        Row(arr)
      }
    }
    val manager = ctx.sparkThread.getDDFManager

    val columns = ddf.getSchemaHandler.getColumns
    val schema = new Schema(columns)
    val newDDF = manager.newDDF(manager, newRDD, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    manager.addDDF(newDDF)
    (new DataFrameResult(newDDF), keyValue)
  }
}

object TransformFactor {
  def getColFactors(ddf: DDF): List[Column] = {
    ddf.getSchemaHandler.getColumns.filter {
      col => {
        (col.getOptionalFactor != null) || (col.getType == Schema.ColumnType.STRING)
      }
    } .toList
  }

  def getFactor(ddf: DDF, ctx: ExecutionContext): Array[(JInt, java.util.Map[String, JInt])] = {
//    val colFactors = TransformFactor.getColFactors(ddf)
//
//    //is String column but don't have factor, so have to get factor
//    val toGetFactorColumns = colFactors.filter {
//      column => (column.getType == Schema.ColumnType.STRING) && (column.getOptionalFactor == null)
//    }.map{col => ddf.getColumnIndex(col.getName)}
//    new GetMultiFactor(ddf.getName, toGetFactorColumns.toArray).run(ctx).result
//
//    val result = for {
//      column <- colFactors
//    } yield (JInt.valueOf(ddf.getColumnIndex(column.getName)), column.getOptionalFactor.getLevelCounts)
//    result.toArray
    ddf.getSchemaHandler.setFactorLevelsForStringColumns(ddf.getSchemaHandler.getColumns.map{col => col.getName}.toArray)
    ddf.getSchemaHandler.computeFactorLevelsAndLevelCounts()
    val colFactors = ddf.getSchemaHandler.getColumns.filter {
      col => col.getOptionalFactor != null
    }
    val result = for {
      column <- colFactors
    } yield (JInt.valueOf(ddf.getColumnIndex(column.getName)), column.getOptionalFactor.getLevelCounts)
    result.toArray
  }
}
