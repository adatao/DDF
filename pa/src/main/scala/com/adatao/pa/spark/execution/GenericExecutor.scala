package com.adatao.pa.spark.execution

import com.adatao.pa.spark.execution.GenericExecutor.GenericExecutorResult
import com.google.gson.{Gson, JsonObject}
import java.lang.reflect.Method
/**
 * author: daoduchuan
 */
class GenericExecutor(command: String, ddfName: String = null, params: Array[String]) extends AExecutor[String] {

  override def runImpl(context: ExecutionContext): String = {
    val manager = context.sparkThread.getDDFManager
    val commandsChain = command.split(".")

    val resultObj = if(ddfName != null) {
      val ddf = manager.getDDF(ddfName)
      GenericExecutor.parseCommand(ddf, commandsChain, params)
    }  else {
      GenericExecutor.parseCommand(manager, commandsChain, params)
    }
    val gson = new Gson()

    gson.toJson(resultObj)
  }
}

object GenericExecutor {
  class GenericExecutorResult {

  }

  def parseCommand(startingObj: Object, commandChain: Array[String], params: Array[String]): Object = {
    var obj = startingObj
    var method: java.lang.reflect.Method = null
    for(i <- 0 until commandChain.size - 1) {
      method = obj.getClass.getMethod(commandChain(i), null)
      obj = method.invoke(obj, null)
    }
    method = obj.getClass.getMethod(commandChain.last, params.getClass)
    method.invoke(obj, params)
  }
}
