package com.adatao.pa.spark.DDF

import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.ddf.ml.{RocMetric, IModel}
import com.adatao.pa.spark.execution._
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.NRow.NRowResult
import com.adatao.pa.spark.DDF.DDFManager.client
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.pa.spark.DDF.content.Schema

/**
 * author: daoduchuan
 */
class DDF(val name: String, val metainfo: Array[MetaInfo]) {

  val ML: MLFacade = new MLFacade(this)

  val Schema: Schema = new Schema(this)

  def getColumnNames(): Array[String] = {
    Schema.getColumnNames()
  }

  def getNumRows(): Long = {
    Schema.getNumRows()
  }

  def getNumColumns(): Int = {
    Schema.getNumColumns()
  }

  def summary(): DataframeStatsResult = {
    val cmd = new QuickSummary
    cmd.setDataContainerID(this.name)
    client.execute[DataframeStatsResult](cmd).result
  }

  def applyModel(model: IModel): DDF = {
    val cmd = new YtrueYpred(this.name, model.getName)
    val result = client.execute[YtrueYpredResult](cmd).result
    new DDF(result.dataContainerID, result.metaInfo)
  }

  def roc(alpha_length: Int): RocMetric = {
    val cmd = new ROC(this.name, alpha_length)
    client.execute[RocMetric](cmd).result
  }

  def setMutable(isMutable: Boolean): DDF = {
    val cmd = new MutableDDF(this.name, isMutable)
    val result = client.execute[Sql2DataFrameResult](cmd).result
    new DDF(result.dataContainerID, result.metaInfo)
  }

  def transform(transformExp: String): DDF = {
    val cmd = new TransformNativeRserve(this.name, transformExp)
    val dataFrameResult = client.execute[DataFrameResult](cmd).result
    new DDF(dataFrameResult.dataContainerID, dataFrameResult.metaInfo)
  }

  def binning(column: String, binningType: String, numBins: Int = 0, breaks: Array[Double] = null,
               includeLowest: Boolean = false, right: Boolean= true, decimalPlaces: Int = 2): DDF = {
    val cmd = new Binning(this.name, column, binningType, numBins, breaks, includeLowest, right, decimalPlaces)
    val result = client.execute[BinningResult](cmd).result
    new DDF(result.dataContainerID, result.metaInfo)
  }
}

object DDF {


}
