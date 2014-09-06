package com.adatao.pa.spark.DDF

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.gson.Gson
import com.adatao.pa.thrift.Server
import com.adatao.pa.thrift.generated.JsonCommand
import com.adatao.pa.thrift.generated.RCommands
import com.adatao.pa.thrift.generated.JsonResult
import com.adatao.pa.spark.MultiContextConnectResult
import com.adatao.pa.spark.execution.LoadTable
import com.adatao.pa.spark.execution.Sql2DataFrame
import com.adatao.pa.spark.execution.Sql2ListString
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.pa.spark.execution.Sql2ListString.Sql2ListStringResult
import org.json.JSONObject
import com.adatao.pa.spark.types._
import com.adatao.pa.spark.types.FailResult
/**
 * Simulates an RClient communicating with BigR/server via Thrift
 *
 * @author ctn
 *
 */
class ManagerClient(serverHost: String, serverPort: Int) {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass())

  private var sid: String = null
  private var workerPort: Int = 0
  private var workerHost: String = null
  //	private var thriftTransport: TSocket = null
  private var thriftClienttoServer: RCommands.Client = null
  private var thriftClienttoWorker: RCommands.Client = null

  if (System.getProperty("bigr.multiuser", "false").toBoolean) {
    val thriftTransport = new TSocket(serverHost, serverPort);
    thriftTransport.open
    val protocol = new TBinaryProtocol(thriftTransport)
    thriftClienttoServer = new RCommands.Client(protocol)
    // thriftClienttoWorker will be created after connect
  }
  else {
    val thriftTransport = new TSocket(serverHost, serverPort);
    thriftTransport.open
    val protocol = new TBinaryProtocol(thriftTransport)
    thriftClienttoServer = new RCommands.Client(protocol)
    thriftClienttoWorker = thriftClienttoServer
  }

  /**
   * Update thrift client to connect to the right host and port
   */
  def updateThriftClientWorker() = {
    if (thriftClienttoWorker != null)
      thriftClienttoWorker.getOutputProtocol().getTransport().close()

    val thriftTransport = new TSocket(workerHost, workerPort);
    thriftTransport.open
    val protocol = new TBinaryProtocol(thriftTransport)
    thriftClienttoWorker = new RCommands.Client(protocol)
  }

  /**
   * Connects to the BigR server
   */
  def connect(params: String = null) = {
    if (System.getProperty("bigr.multiuser", Server.MULTIUSER_DEFAULT.toString).toBoolean) {
      val res = this.execute[MultiContextConnectResult](thriftClienttoServer, "connect", params)
      sid = res.result.sessionID
      workerPort = res.result.thriftPort
      workerHost = res.result.host
      //LOG.info("connect: SID=%s host=%s thriftPort=%d".format(sid, workerHost, workerPort))
      // connect to worker now
      //			Thread.sleep(5000)
      updateThriftClientWorker()
    }
    else {
      sid = this.executeImpl(thriftClienttoServer, "connect", params).sid
      //LOG.info("connect: SID=%s".format(sid))
    }
  }

  /**
   * Disconnects from the BigR server
   */
  def disconnect = {
    LOG.info("disconnect: %s".format(this.executeImpl(thriftClienttoServer, "disconnect", null).toString))
  }

  /**
   * Executes a command given a command object with its parameters already set, and which will be serialized
   * before being sent to the BigR Thrift server
   *
   * @return ExecutionResult[T]
   */
  def execute[T](commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] =
    execute[T](thriftClienttoWorker, commandObject)

  def execute[T](thriftClient: RCommands.Client, commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] = {
    this.execute[T](thriftClient, commandObject.getClass.getSimpleName, new Gson().toJson(commandObject))
  }

  def executeNew[T](commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] =
    executeNew[T](thriftClienttoWorker, commandObject)

  def executeNew[T](thriftClient: RCommands.Client, commandObject: Object)(implicit m: Manifest[T]): ExecutionResult[T] = {

    var b = new JSONObject(new Gson().toJson(commandObject))
    b.put("methodName", commandObject.getClass.getSimpleName)
    b.put("ddfUri", commandObject.getClass.getSimpleName)
    println(">>>newjson b: " + b)
    this.execute[T](thriftClient, "DDFExecutor", b.toString())

    //		this.execute[T](thriftClient, commandObject.getClass.getSimpleName, new Gson().toJson(commandObject))

  }

  /**
   * Executes a command string together with supplied parameters. Note that this is NOT
   * compatible with calling [[CExecutor]]s, since they do not return an [[ExecutionResult]] object.
   * An exception will be thrown when attempting to deserialize results from [[CExecutor]]s.
   *
   * The recommended way of working with old [[CExecutor]] is to call executeNoWrap().
   *
   * @return ExecutionResult[T]
   */
  def execute[T](commandName: String, params: String)(implicit m: Manifest[T]): ExecutionResult[T] =
    execute[T](thriftClienttoWorker, commandName, params)

  def execute[T](thriftClient: RCommands.Client, commandName: String, params: String)(implicit m: Manifest[T]): ExecutionResult[T] = {
    val resultJson = this.executeImpl(thriftClient, commandName, params).getResult

    if (false && classOf[ExecutorResult].isAssignableFrom(m.erasure)) {
      // This is expecting a result from the old, deprecated CExecutor
      ExecutionResult.fromJson[T](resultJson) match {
        case sr: SuccessResult ⇒ new SuccessfulResult(sr.asInstanceOf[T])
        case fr: FailResult ⇒ new FailedResult(fr.message)
        case _ ⇒ new FailedResult("Unable to parse result JSON: %s".format(resultJson))
      }
    }
    else {
      // New-style AExecutor already returns result wrapped inside an ExecutionResult (either SuccessfulResult or FailedResult)
      ExecutionResult.fromJson[T](resultJson)
    }
  }

  private def executeImpl(commandName: String, params: String): JsonResult =
    executeImpl(thriftClienttoWorker, commandName, params)

  private def executeImpl(thriftClient: RCommands.Client, commandName: String, params: String): JsonResult = {
    if(thriftClient == null) {
      println(">>>>>>> thriftClient is null")
    }
    val json = thriftClient.execJsonCommand(new JsonCommand().setCmdName(commandName).setSid(sid).setParams(params))
    if (json != null && json.getResult != null && json.getResult.matches(".*\"success\":false.*")) throw new Exception(json.getResult)
    json
  }

  //new DDFExecutor, call DDFExecutor instead of old executor
  private def executeImplnew(thriftClient: RCommands.Client, commandName: String, params: String): JsonResult = {
    val json = thriftClient.execJsonCommand(new JsonCommand().setCmdName("DDFExecutor").setSid(sid).setParams(params))
    if (json != null && json.getResult != null && json.getResult.matches(".*\"success\":false.*")) throw new Exception(json.getResult)
    json
  }
}

/**
 * Companion object/singleton
 */
object ManagerClient {

}

object BigRThriftServerUtils {
  var thriftServer: Server = null
  //	var thriftTransport: TTransport = null
  val HOST = "localhost"
  val PORT = 7912

  def startServer: ManagerClient = {
    thriftServer = new Server(PORT);

    new Thread(new Runnable() { def run = thriftServer.start }).start;
    Thread.sleep(5000)
    try {
      new ManagerClient(HOST, PORT)
    }
    catch {
      case e: TTransportException ⇒ {
        e.printStackTrace
        assert(false)
        null
      }
    }
  }

  def stopServer = {
    thriftServer.stop;
    //		thriftTransport.close
  }

  def createClient: ManagerClient = {
    new ManagerClient(HOST, PORT)
  }
}

object ManagerClientTestUtils {
  /**
   * Load one of the file paths given and returns the first successful load
   *
   * @param fileUrls list of file URLs to try one after another, until successful
   */
  def loadFile(bigRClient: ManagerClient, fileUrls: List[String], hasHeader: Boolean, fieldSeparator: String, sampleSize: Int): String = {
    fileUrls.foreach {
      fileUrl ⇒
        try {
          return this.loadFile(bigRClient, fileUrl, hasHeader, fieldSeparator, sampleSize)
        }
        catch {
          case e: Exception ⇒ println("Ignored Exception: %s".format(e.getMessage()))
        }
    }
    null
  }

  /**
   * Connects to an existing session and loads the given fileUrl
   *
   * @param sessionId
   * @param fileUrl
   * @return dataContainerId
   */
  def loadFile(bigRClient: ManagerClient, fileUrl: String, hasHeader: Boolean, fieldSeparator: String, sampleSize: Int): String = {
    bigRClient.execute[LoadTable.LoadTableResult](
      new LoadTable().setFileURL(fileUrl).setHasHeader(hasHeader).setSeparator(fieldSeparator).setSampleSize(sampleSize)).result.getDataContainerID
  }

  def runSQLCmd(bigRClient: ManagerClient, cmdStr: String): Sql2ListStringResult = {
    val sql: Sql2ListString = new Sql2ListString().setSqlCmd(cmdStr)
    bigRClient.execute[Sql2ListStringResult](sql).result
  }

  def runSQL2RDDCmd(bigRClient: ManagerClient, cmdStr: String, cache: Boolean): Sql2DataFrameResult = {
    val sql: Sql2DataFrame = new Sql2DataFrame(cmdStr, cache)
    bigRClient.execute[Sql2DataFrameResult](sql).result
  }
}
