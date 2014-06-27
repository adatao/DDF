package com.adatao.pa.spark.scalaClient

import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.ddf.ml.{RocMetric, IModel}
import com.adatao.pa.spark.execution._
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.NRow.NRowResult

/**
 * author: daoduchuan
 */
class DDF(client: BigRClient2, val dataContainerID: String, val metainfo: Array[MetaInfo]) {

  def getNumRows(): Long = {
    val cmd = new NRow
    cmd.setDataContainerID(this.dataContainerID)
    val result = client.execute[NRowResult](cmd).result
    result.nrow
  }

  def summary(): DataframeStatsResult = {
    val cmd = new QuickSummary
    cmd.setDataContainerID(this.dataContainerID)
    client.execute[DataframeStatsResult](cmd).result
  }

  def applyModel(model: IModel) = {
    val cmd = new YtrueYpred(this.dataContainerID, model.getName)
    val result = client.execute[YtrueYpredResult](cmd).result
    new DDF(client, result.dataContainerID, result.metaInfo)
  }

  def roc(alpha_length: Int): RocMetric = {
    val cmd = new ROC(this.dataContainerID, alpha_length)
    client.execute[RocMetric](cmd).result
  }

  def transform(transformExp: String): DDF = {
    val cmd = new TransformNativeRserve(this.dataContainerID, transformExp)
    val dataFrameResult = client.execute[DataFrameResult](cmd).result
    new DDF(client, dataFrameResult.dataContainerID, dataFrameResult.metaInfo)
  }
}

object DDF {


}
