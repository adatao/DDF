package com.adatao.pa.spark.DDF.content
import com.adatao.pa.spark.DDF.DDF

import com.adatao.pa.spark.execution.GetMultiFactor
import com.adatao.pa.spark.DDF.DDFManager.client
import java.util.{ Map => JMap}
import java.util
import scala.collection.Map
import java.lang.{ Integer => JInt }

/**
 * author: daoduchuan
 */
class SchemaHandler(val ddf: DDF) {

  var factorColumns: Array[Int] =  Array[Int]()

  var levelCounts: Map[JInt, JMap[String, JInt]] = null

  def setAsFactor(colName: String): SchemaHandler = {
    val colIdx = this.ddf.getSchema().getColumnIndex(colName)
    this.setAsFactor(colIdx)
  }

  def setAsFactor(colIdx: Int): SchemaHandler = {
    factorColumns = factorColumns :+ colIdx
    this
  }

  def computeLevelsCount(): SchemaHandler = {
    val cmd = new GetMultiFactor(this.ddf.name, factorColumns)
    val result = client.execute[Array[(JInt, JMap[String, JInt])]](cmd).result
    this.levelCounts = result.toMap
    this
  }

  def getLevelCounts(colName: String): JMap[String, JInt] = {
    val colIdx = this.ddf.getSchema().getColumnIndex(colName)
    if(!factorColumns.exists(col => col == colIdx)) {
      factorColumns = factorColumns :+ colIdx
      computeLevelsCount()
    } else if(levelCounts == null) {
      computeLevelsCount()
    }
    levelCounts(colIdx)
  }
}

object SchemaHandler {


}