package com.adatao.spark.ddf.util

import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import shark.{KryoRegistrator, SharkContext}
import org.apache.spark.SparkConf

/**
 */

object SparkUtils {
  def createSparkConf(master: String, jobName: String, sparkHome: String, jars: Array[String],
                      environment: JMap[String, String]): SparkConf = {

    val conf = SharkContext.createSparkConf(master, jobName, sparkHome, jars, environment.asScala)
    conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", classOf[KryoRegistrator].getName))
  }

  def createSharkContext(master: String, jobName: String, sparkHome: String, jars: Array[String],
                         environment: JMap[String, String]): SharkContext = {
    val conf = createSparkConf(master, jobName, sparkHome, jars, environment)
    new SharkContext(conf)
  }
}
