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
 */
class TransformHMap(dataContainerID: String, keyValMap: Array[(JInt, java.util.Map[String, JDouble])]) extends AExecutor[DataFrameResult] {

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val ddf = ctx.sparkThread.getDDFManager.getDDF(dataContainerID)
    val keyValueMap = keyValMap.toMap

    val colFactorIndexes: List[Int] = keyValMap.map{case (idx, hmap) => idx.toInt}.toList

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
    new DataFrameResult(newDDF)
  }
}
