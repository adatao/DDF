package com.adatao.pa.ddf.spark

import com.adatao.pa.spark.execution._
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import io.ddf.ml.IModel
import com.adatao.pa.spark.Utils.{DataFrameResult, MutableDataFrameResult}
import io.ddf.DDF.DDFInformation
import com.adatao.pa.spark.DDF.ManagerClient
import com.adatao.pa.ddf.spark.DDFManager.client
import java.util.List


class DDFManager() {

  var nameSpace: String = "adatao"

  def connect(serverHost: String, serverPort: Int = 7911) = {
    DDFManager.createClient(serverHost, serverPort)
  }

  def setNameSpace(ns: String) {
    nameSpace = ns
  }
  
  def getNameSpace(): String= {
    nameSpace 
  }

  def sql2ddf(command: String): DDF = {
    val cmd = new Sql2DataFrame(command, true)
    val result = client.execute[Sql2DataFrameResult](cmd)
    new DDF(result.result.dataContainerID, result.result.metaInfo)
  }

  def cql2ddf(cqlCommand: String): DDF = {
    val cmd = new CQL2DDF(cqlCommand)
    val result = client.execute[DataFrameResult](cmd)
    new DDF(result.result.dataContainerID, result.result.metaInfo)
  }

  def cql2txt(cqlCommand: String): List[String] = {
    val cmd = new CQL2TXT(cqlCommand)
    val result = client.execute[List[String]](cmd).result
    result
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

  def loadModel(uri: String): IModel = {
    val cmd = new LoadModel(uri)
    client.execute[IModel](cmd).result
  }
}

object DDFManager {

  var client: ManagerClient = null

  private def createClient(serverHost: String, serverPort: Int = 7911) = {
    client = new ManagerClient(serverHost, serverPort)
    client.connect()
  }

  def apply(serverHost: String, serverPort: Int = 7911): DDFManager = {
    val manager = new DDFManager()
    manager.connect(serverHost, serverPort)
    manager
  }

  def get(section: String): DDFManager = {
    if(section.toLowerCase() != "spark") {
      throw new Exception("Only section \"spark\" is available")
    }
    new DDFManager()
  }
}
