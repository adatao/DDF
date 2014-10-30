package com.adatao.spark.ddf

import io.spark.ddf.{SparkDDF => IOSparkDDF, SparkDDFManager}
import io.ddf.DDFManager
import io.ddf.content.Schema
import org.apache.spark.sql.columnar.{CachedBatch, InMemoryRelation}
import io.ddf.exception.DDFException
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import org.apache.spark.sql.CachedData

/**
 */
class SparkDDF(manager: DDFManager, data: AnyRef,
               typeSpecs: Array[Class[_]], namespace: String,
               name: String, schema: Schema) extends IOSparkDDF(manager, data, typeSpecs, namespace, name, schema) {
  //Cache table force the materializing of RDD[Array[ByteBuffer]]
  // also add RDd[Array[ByteBuffer]] into RepHandler
  override def cacheTable(): Unit = {
    this.saveAsTable()
    val hiveContext = this.getManager.asInstanceOf[SparkDDFManager].getHiveContext
    hiveContext.cacheTable(this.getTableName)
//    val inMemoryRelation = hiveContext.table(this.getTableName).queryExecution.analyzed match {
//      case inMemory: InMemoryRelation => inMemory
//      case something => throw new DDFException("Not InMemory Relation, class = " + something.getClass.toString)
//    }
    val cachedData: CachedData = hiveContext.lookupCachedData(hiveContext.table(this.getTableName)) match {
      case Some(cachedData) => cachedData
      case None => throw new DDFException("Not InMemory Relation")
    }
//    val cachedData = hiveContext.table(this.getTableName).queryExecution.withCachedData.collect {
//      case inMemoryRelation: InMemoryRelation => inMemoryRelation
//    }
//    mLog.info(">>>> cachedData.size = "  + cachedData.size)

    val cachedBatch: RDD[CachedBatch] = cachedData.cachedRepresentation.cachedColumnBuffers
    //force the table to materialzie
    mLog.info(">>>>> force the table to materialize")
    cachedBatch.count()
    this.getRepresentationHandler.add(cachedBatch, classOf[RDD[_]], classOf[CachedBatch])
  }
  def this(manager: DDFManager) = {
    this(manager, null, null, manager.getNamespace, null, null)
  }
}
