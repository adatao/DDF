package com.adatao.pa.spark.execution

import com.adatao.pa.spark.execution.GenericExecutor.GenericExecutorResult
import com.google.gson.{Gson, JsonObject}
import java.lang.reflect.Method
import com.adatao.ML.Utils
import com.adatao.ddf.util.Utils.ClassMethod
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * author: daoduchuan
 */
class GenericExecutor(command: String, dcID: String = null, params: Array[String] = Array()) extends AExecutor[String] {

  override def runImpl(context: ExecutionContext): String = {
    val manager = context.sparkThread.getDDFManager
    val commandsChain = command.split('.')
    val ddfName = Utils.dcID2DDFID(dcID)

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
//  class GenericClassMethod(theObject: Object, methodName: String, methodArgs: Array[String]) extends
//    ClassMethod(theObject, methodName) {
//
//  }

  def parseCommand(startingObj: Object, commandChain: Array[String], params: Array[String]): Object = {
    var obj = startingObj
    var method: java.lang.reflect.Method = null
    for(i <- 1 until commandChain.size - 1) {
      method = obj.getClass.getMethod(commandChain(i))
      println(s">>>>> method = ${method.getName}")
      obj = method.invoke(obj)
    }
    println(s">>>> commandChain = ${commandChain.mkString(", ")}")

    val classMethods = obj.getClass.getMethods
    val methodName = commandChain.last
    val foundMethod = classMethods.filter(method => method.getName == methodName).find(method => method.getParameterTypes.size == params.size)

//    foundMethod match {
//      case Some(m) => {
//        val methodArgTypes = m.getParameterTypes
//
//      }
//      case None => throw new AdataoException(AdataoExceptionCode,
//        s"Cannot find method $methodName from command ${commandChain.mkString(".")}", null)
//    }
    foundMethod.map{
      m => {}
    }
    //method = obj.getClass.getMethod(commandChain.last)
    method.invoke(obj)
  }
}
