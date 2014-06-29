package com.adatao.pa.spark.DDF.content

import com.adatao.ddf.DDF
import scala.collection.mutable.ListBuffer
/**
 * author: daoduchuan
 */
class SchemaHandler(val ddf: DDF) {

  var factorColumns: Array[Int] =  Array[Int]()

  def setAsFactor(colName: String) = {
    val colIdx = this.ddf.getColumnIndex(colName)
    this.setAsFactor(colIdx)
  }

  def setAsFactor(colIdx: Int) = {
    this.factorColumns = this.factorColumns :+ colIdx
  }

}
