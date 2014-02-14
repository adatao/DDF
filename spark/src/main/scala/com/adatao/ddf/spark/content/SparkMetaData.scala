package com.adatao.ddf.spark.content

import com.adatao.ddf.content.AMetaDataHandler
import com.adatao.ddf.spark.SparkDDFManager
import com.adatao.ddf.spark.etl.SparkPersistenceHandler

/**
 * author: daoduchuan
 */
class SparkMetaDataHandler(ddfManager: SparkDDFManager) extends AMetaDataHandler(ddfManager){

  override def getNumRowsImpl(): Long = {

    val tablename= ddfManager.getMetaDataHandler.getSchema.getTableName
    val cmd= "select count(*) from " + tablename
    try{
      ddfManager.getPersistenceHandler.asInstanceOf[SparkPersistenceHandler].sql(tablename)(0).toLong
    }
    catch {
      case e => throw new Exception("Cannot get number of rows")
    }
  }

}
