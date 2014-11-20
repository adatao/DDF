package com.adatao.spark.ddf

import io.spark.ddf.{SparkDDFManager => IOSparkManager}
import java.lang.{ Integer => JInt }
import java.lang.{Double => JDouble}
import scala.collection.mutable.Map
import io.ddf.exception.DDFException
import java.util.UUID

/**
 * author: daoduchuan
 */
class SparkDDFManager extends IOSparkManager {

  private val hmap : Map[String, Array[(JInt, java.util.Map[String, JDouble])]] =
     Map[String, Array[(JInt, java.util.Map[String, JDouble])]]()

  def putMap(value: Array[(JInt, java.util.Map[String, JDouble])]): String = {
    val key = UUID.randomUUID().toString
    mLog.info(">>>> put hmap into SparkDDFManager " + key)
    hmap.put(key, value)
    key
  }

  def getMap(key: String): Array[(JInt, java.util.Map[String, JDouble])] = hmap.get(key) match {
    case Some(hmap) => hmap
    case None => throw new DDFException("Error getting key -> value map")
  }
}
