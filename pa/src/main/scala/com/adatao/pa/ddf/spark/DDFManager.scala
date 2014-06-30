package com.adatao.pa.ddf.spark

import com.adatao.pa.spark.execution.{ListDDF, GetDDF, LoadModel, Sql2DataFrame}
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.ddf.ml.IModel
import com.adatao.pa.spark.Utils.MutableDataFrameResult
import com.adatao.ddf.DDF.DDFInformation
import com.adatao.pa.spark.DDF.ManagerClient
import com.adatao.pa.ddf.spark.DDFManager.client


class DDFManager(serverHost: String, serverPort: Int = 7911) {


  DDFManager.createClient(serverHost, serverPort)
  var nameSpace: String = "com.adatao"
  
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

  def apply(serverHost: String = "pa2.adatao.com", serverPort: Int = 7911): DDFManager = {
    new DDFManager(serverHost, serverPort)
  }

  def get(section: String): DDFManager = {
    if(section != "spark") {
      throw new Exception("Sorry only \"spark\" section is available now")
    }

    val cluster = System.getProperty("DDF_CLUSTER", "pa2.adatao.com")
    val m = try {
      new DDFManager(cluster, 7911)
    } catch {
      case e: Throwable => new DDFManager("pa4.adatao.com", 7911)
    }
    System.out.println("[NameSpace]: " + m.getNameSpace);
    m
  }
}
