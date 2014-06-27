package com.adatao.pa.spark.scalaClient

import com.adatao.pa.spark.execution.{LoadModel, Sql2DataFrame}
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.ddf.ml.IModel


class Manager(serverHost: String, serverPort: Int) {

  val client: BigRClient2 = new BigRClient2(serverHost, serverPort)
  client.connect()

  def sql2ddf(command: String): DDF = {
    val cmd = new Sql2DataFrame(command, true)
    val result = client.execute[Sql2DataFrameResult](cmd)
    new DDF(client, result.result.dataContainerID, result.result.metaInfo)
  }

  def loadModel(modelName: String): IModel = {
    val cmd = new LoadModel(modelName)

    client.execute[IModel](cmd).result
  }
}

object Manager {

}
