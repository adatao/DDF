package com.adatao.spark.ddf

import io.spark.ddf.{SparkDDF => IOSparkDDF, SparkDDFManager}
import io.ddf.DDFManager
import io.ddf.content.Schema
import org.apache.spark.sql.columnar.InMemoryRelation
import io.ddf.exception.DDFException
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer

/**
 */
//(manager : DDFManager, data : AnyRef, typeSpecs : Array[Class[_]], namespace : String, name : String, schema : Schema)
class SparkDDF(manager: DDFManager, data: AnyRef,
               typeSpecs: Array[Class[_]], namespace: String,
               name: String, schema: Schema) extends IOSparkDDF(manager, data, typeSpecs, namespace, name, schema) {

  override def cacheTable(): Unit = {
    this.saveAsTable()
    val hiveContext = this.getManager.asInstanceOf[SparkDDFManager].getHiveContext
    hiveContext.cacheTable(this.getTableName)
    val inMemoryRelation = hiveContext.table(this.getTableName).queryExecution.analyzed match {
      case inMemory: InMemoryRelation => inMemory
      case something => throw new DDFException("Not InMemory Relation, class = " + something.getClass.toString)
    }
    val cachedColumnBuffers= inMemoryRelation.cachedColumnBuffers
    //force the table to materialzie
    cachedColumnBuffers.count()
    this.getRepresentationHandler.add(cachedColumnBuffers, classOf[RDD[_]], classOf[Array[ByteBuffer]])
  }
}
