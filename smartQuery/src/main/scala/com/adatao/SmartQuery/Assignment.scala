package com.adatao.SmartQuery

import scala.collection.mutable.Map
import com.adatao.pa.ddf.spark.DDF

/**
 * author: daoduchuan
 */
case class Assignment(variableName: String) {
  def assign(value: AnyRef) = {

    Environment.environment(variableName) = value
  }
}

object Environment {
  val datasets: Map[String, DDF] = Map[String, DDF]()

  var currentDDF: DDF = null

  def checkValidColumns(columns: List[String]): Boolean = {

    if(currentDDF == null){
      false
    } else {
      val cols = currentDDF.getColumnNames()
      columns.forall(col => cols.contains(col))
    }
  }
  val environment: Map[String, AnyRef] = Map[String, AnyRef]()
}
