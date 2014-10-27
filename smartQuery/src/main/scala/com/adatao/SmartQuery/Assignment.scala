package com.adatao.SmartQuery

import scala.collection.mutable.Map
/**
 * author: daoduchuan
 */
case class Assignment(variableName: String) {
  def assign(value: AnyRef) = {

    Environment.environment(variableName) = value
  }
}

case class GetVariable(variableName: String) {

  def get(): AnyRef = {
    try{
      Environment.environment(variableName)
    } catch {
      case e: Exception => throw new Exception("Error getting variable")
    }
  }
}

object Environment {
  val environment: Map[String, AnyRef] = Map[String, AnyRef]()
}
