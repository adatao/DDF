package com.adatao.pa.spark.execution

import scala.collection.JavaConversions._

import java.util.{ HashMap => JMap, Map }
import java.util
import java.lang.{ Integer => JInt }
import com.adatao.pa.spark.DataManager.{ MetaInfo, DataFrame}

import com.adatao.pa.spark.Utils._
import scala.Some
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

import com.adatao.spark.ddf.analytics.Utils

/**

 *
 */
class GetMultiFactor(dataContainerID: String,
  var columnIndexs: Array[Int] = null)
  extends AExecutor[Array[(JInt, java.util.Map[String, JInt])]] {

  protected override def runImpl(context: ExecutionContext): Array[(JInt, java.util.Map[String, JInt])] = {

    val ddf = context.sparkThread.getDDFManager.getDDF(dataContainerID)

    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF dataContainerId= " + dataContainerID + "\t ddfId = " + dataContainerID, null)
    }
    val schemaHandler = ddf.getSchemaHandler
    for (columnIndex <- columnIndexs) {
      schemaHandler.setAsFactor(columnIndex)
    }
    schemaHandler.computeFactorLevelsAndLevelCounts()

    val result: Array[(JInt, java.util.Map[String, JInt])] = for {
      columnIndex <- columnIndexs
      colName = schemaHandler.getColumnName(columnIndex)
    } yield ((JInt.valueOf(columnIndex), schemaHandler.getColumn(colName).getOptionalFactor.getLevelCounts))

    result
  }
}

