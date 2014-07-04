package com.adatao.pa.spark.DDF.content

import com.adatao.pa.spark.execution.NRow
import com.adatao.pa.ddf.spark.{DDF, DDFManager}
import com.adatao.pa.ddf.spark.DDFManager.client
import com.adatao.pa.spark.execution.NRow.NRowResult
import com.adatao.ddf.content.Schema.Column

/**
 * author: daoduchuan
 */
class Schema(ddf: DDF, val columns: Array[Column]) {

  def getNumRows(): Long = {
    val cmd = new NRow
    cmd.setDataContainerID(this.ddf.name)
    val result = client.execute[NRowResult](cmd).result
    result.nrow
  }

  def getNumColumns(): Int = {
    return this.ddf.columns.size
  }

  def getColumnName(idx: Int): String = {
    this.ddf.columns(idx).getName
  }

  def getColumnNames(): Array[String] = {
    this.ddf.columns.map{col => col.getName}
  }

  def getColumnIndex(colName: String): Int = {
    val metaInfoWIndex = this.ddf.columns.zipWithIndex
    metaInfoWIndex.find(p => p._1.getName == colName) match {
      case Some(col) => col._2
      case None      => throw new Exception(s"Cannot find column $colName")
    }
  }

  def getColumn(colName: String): Column = {
    val colIdx = this.getColumnIndex(colName)
    columns(colIdx)
  }

  def getDDFName(): String = {
    this.ddf.name
  }
}
