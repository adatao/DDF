package com.adatao.spark.ddf.content

import com.adatao.ddf.content.AMetaDataHandler
import com.adatao.spark.ddf.SparkDDFManager
import com.adatao.spark.ddf.etl.SparkDataCommandHandler
import com.adatao.ddf.exception.DDFException

//import com.adatao.ddf.spark.etl.SparkPersistenceHandler

/**
 * author: daoduchuan
 */
class SparkMetaDataHandler(ddfManager: SparkDDFManager) extends AMetaDataHandler(ddfManager){

  override def getNumRowsImpl(): Long = {

    val tablename= this.getSchema.getTableName
    val cmd= "select count(*) from " + tablename
    try{
      ddfManager.
        getDataCommandHandler.
        asInstanceOf[SparkDataCommandHandler].
        cmd2txt("selecy count(*) from " + tablename).get(0).toLong
    }
    catch {
      case e => throw new DDFException("Cannot get number of rows")
    }
  }
}
