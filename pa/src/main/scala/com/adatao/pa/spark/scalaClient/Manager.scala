package com.adatao.pa.spark.scalaClient

import com.adatao.pa.spark.execution.{ListDDF, GetDDF, LoadModel, Sql2DataFrame}
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.ddf.ml.IModel
import com.adatao.pa.spark.Utils.MutableDataFrameResult
import com.adatao.ddf.DDF.DDFInformation
import com.adatao.pa.spark.scalaClient.Manager.client


class Manager(serverHost: String, serverPort: Int = 7911) {


  Manager.createClient(serverHost, serverPort)

  def sql2ddf(command: String): DDF = {
    val cmd = new Sql2DataFrame(command, true)
    val result = client.execute[Sql2DataFrameResult](cmd)
    new DDF(result.result.dataContainerID, result.result.metaInfo)
  }

  def getDDF(ddfName: String): DDF = {
    val cmd = new GetDDF(ddfName)
    val result = client.execute[MutableDataFrameResult](cmd).result
    new DDF(result.getDataContainerID, result.metaInfo)
  }

  def listDDFs(): String =  {
    val cmd = new ListDDF
    val result = client.execute[Array[DDFInformation]](cmd).result
    result.map(ddfInfo => ddfInfo.getUri).mkString("\n")
  }

  def loadModel(modelName: String): IModel = {
    val cmd = new LoadModel(modelName)
    client.execute[IModel](cmd).result
  }
}

object Manager {

  var client: ManagerClient = null

  private def createClient(serverHost: String, serverPort: Int = 7911) = {
    client = new ManagerClient(serverHost, serverPort)
    client.connect()
  }

  def apply(serverHost: String, serverPort: Int = 7911): Manager = {
    new Manager(serverHost, serverPort)
  }

  def get(section: String): Manager = {
    new Manager("pa4.adatao.com", 7911)
  }
}
