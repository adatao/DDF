package com.adatao.pa.spark.DDF.content

import com.adatao.pa.spark.execution.NRow
import com.adatao.pa.spark.DDF.DDFManager._
import com.adatao.pa.spark.execution.NRow.NRowResult
import com.adatao.pa.spark.DDF.DDF

/**
 * author: daoduchuan
 */
class Schema(ddf: DDF) {

  def getNumRows(): Long = {
    val cmd = new NRow
    cmd.setDataContainerID(this.ddf.name)
    val result = client.execute[NRowResult](cmd).result
    result.nrow
  }

  def getNumColumns(): Int = {
    return this.ddf.metainfo.size
  }

  def getColumnName(idx: Int): String = {
    this.ddf.metainfo.apply(idx).getHeader
  }

  def getColumnNames(): Array[String] = {
    this.ddf.metainfo.map{info => info.getHeader}
  }

  def getColumnIndex(colName: String): Int = {
    val metaInfoWIndex = this.ddf.metainfo.zipWithIndex
    metaInfoWIndex.find(p => p._1.getHeader == colName) match {
      case Some(col) => col._2
      case None      => throw new Exception(s"Cannot find column $colName")
    }
  }

  def getDDFName(): String = {
    this.ddf.name
  }
}
