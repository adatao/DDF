package com.adatao.spark.ddf

import io.spark.ddf.{SparkDDF => IOSparkDDF, SparkDDFManager}
import io.ddf.DDFManager
import io.ddf.content.Schema
import org.apache.spark.sql.columnar.{CachedBatch, InMemoryRelation}
import io.ddf.exception.DDFException
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import org.apache.spark.sql.{SchemaRDD, CachedData}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.hive.HiveContext

/**
 */
class SparkDDF(manager: DDFManager, data: AnyRef,
               typeSpecs: Array[Class[_]], namespace: String,
               name: String, schema: Schema) extends IOSparkDDF(manager, data, typeSpecs, namespace, name, schema) {
  //Cache table force the materializing of RDD[Array[ByteBuffer]]
  // also add RDd[Array[ByteBuffer]] into RepHandler
  override def cacheTable(): Unit = {
    val hiveContext = this.getManager.asInstanceOf[SparkDDFManager].getHiveContext
    this.repartition(hiveContext)
    this.saveAsTable()

    hiveContext.cacheTable(this.getTableName)

    val cachedData: CachedData = hiveContext.lookupCachedData(hiveContext.table(this.getTableName)) match {
      case Some(cachedData) => cachedData
      case None => throw new DDFException("Not InMemory Relation")
    }

    val cachedBatch: RDD[CachedBatch] = cachedData.cachedRepresentation.cachedColumnBuffers
    //force the table to materialzie
    mLog.info(">>>>> force the table to materialize")
    cachedBatch.context.runJob(cachedBatch, (iter: Iterator[CachedBatch]) => iter.foreach(_ => Unit))
    //cachedBatch.count()
    this.getRepresentationHandler.add(cachedBatch, classOf[RDD[_]], classOf[CachedBatch])
  }
  def this(manager: DDFManager) = {
    this(manager, null, null, manager.getNamespace, null, null)
  }

  def repartition(hiveCtx: HiveContext): Unit = {
    mLog.info(">>>> repartition the table")
    val blowUpFactor = System.getProperty("pa.blowup.factor", SparkDDF.DEFAULT_BLOWUP_FACTOR).toInt
    mLog.info(">>>> blow up factor = " + blowUpFactor)
    val schemaRDD = this.getRepresentationHandler.get(classOf[SchemaRDD]).asInstanceOf[SchemaRDD]
    val schema = schemaRDD.schema
    val numPartitions = schemaRDD.partitions.size
    val repartitionRDD = schemaRDD.repartition(numPartitions * blowUpFactor)

    val newSchemaRDD =  hiveCtx.applySchema(repartitionRDD, schema)
    this.getRepresentationHandler.remove(classOf[SchemaRDD])
    this.getRepresentationHandler.add(newSchemaRDD, classOf[SchemaRDD])
  }
}

object SparkDDF {
  val DEFAULT_BLOWUP_FACTOR = "4"
}
