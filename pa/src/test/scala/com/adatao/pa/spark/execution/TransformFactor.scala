package com.adatao.pa.spark.execution


import java.lang.{ Integer => JInt }
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import scala.collection.JavaConversions._
import io.ddf.content.Schema
import io.ddf.DDF
import io.ddf.content.Schema.Column

/**
 * author: daoduchuan
 * Transform all factor column to index 0
 * example:
 * {A, B, C, D, A...} => {0, 1, 2, 3, 0, ...}
 * {1997, 2000, 1990, 1993, 2000} => {0, 1, 2, 3, 1, ...}
 */
class TransformFactor(dataContainerID: String) extends AExecutor[Array[(JInt, java.util.Map[String, JInt])]] {

  override def runImpl(ctx: ExecutionContext): Array[(JInt, java.util.Map[String, JInt])] = {
    val ddf = ctx.sparkThread.getDDFManager.getDDF(dataContainerID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find ddf", null)
    }
    TransformFactor.getFactor(ddf, ctx)
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
