package com.adatao.pa.spark.execution

import com.google.gson._
import java.lang.reflect.Method
import com.adatao.ML.Utils
import com.adatao.ddf.util.Utils.ClassMethod
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.ddf.DDF
import java.lang.reflect.Type
import scala.Some
import com.adatao.spark.ddf.SparkDDF


/**
 * author: daoduchuan
 */
class GenericExecutor(dcID: String, command: String, params: Array[String] = Array()) extends AExecutor[Object] {

  override def runImpl(context: ExecutionContext): Object = {
    val manager = context.sparkThread.getDDFManager
    val commandsChain = command.toLowerCase().split('.')
    //val gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()

    commandsChain(0) match {
      case "ddf" => {
        val ddfName = Utils.dcID2DDFID(dcID)
        val ddf = manager.getDDF(ddfName)
        GenericExecutor.parseCommand(ddf, commandsChain.drop(1), params)
      }
      case "manager" => {
        GenericExecutor.parseCommand(manager, commandsChain.drop(1), params)
      }
      case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error parsing command, " +
        "command must start with ddf or manager", null)
    }
  }
}

object GenericExecutor {
//  class GenericClassMethod(theObject: Object, methodName: String, methodArgs: Array[String]) extends
//    ClassMethod(theObject, methodName) {
//
//  }

  def parseCommand(startingObj: Object, commandChain: Array[String], params: Array[String]): Object = {
    var obj = startingObj

    for(i <- 0 until commandChain.size - 1) {
      obj = try {
        val methods = obj.getClass.getMethods
        methods.filter(m => m.getName.toLowerCase() == commandChain(i).toLowerCase()).find(m => m.getParameterTypes.size == 0) match {
          case Some(m) => m.invoke(obj)
          case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "method not found", null)
        }
      } catch {
        case e: Throwable => {
          println(s">>>>>> e = ${e.getMessage}")
          obj.getClass.getFields.find(field => field.getName.toLowerCase() == commandChain(i)) match {
            case Some(field) => field.get(obj)
            case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, s"field not found ${commandChain(i)}", null)
          }
//          val field = obj.getClass.getField(commandChain(i))
//          field.get(obj)

        }
      }
    }

    println(s">>>> commandChain = ${commandChain.mkString(", ")}")

    val classMethods = obj.getClass.getMethods
    val methodName = commandChain.last
    val foundMethods = classMethods.filter(method => method.getName.toLowerCase == methodName.toLowerCase()).filter(method => method.getParameterTypes.size == params.size)
    val gson: Gson = new Gson()
    if(foundMethods.size > 0) {
      var iter = foundMethods.toIterator
      var found = false
      var result: Object = null

      while (iter.hasNext && !found) {
        val method = iter.next()
        val methodArgTypes = method.getParameterTypes
        println(s">>> params = ${params.mkString(", ")}")
        println(s">>> methodArgTypes = ${methodArgTypes.mkString(", ")}")
        val args: Array[Object] = (params zip methodArgTypes).map {
          case (p, clss) => gson.fromJson(p, clss).asInstanceOf[java.lang.Object]
        }

        try {
          result = if (args.size > 0) method.invoke(obj, args: _*) else method.invoke(obj)
          found = true
        } catch {
          case e: Throwable => {
            if (!iter.hasNext) {
              throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Cannot find method ", e.getCause)
            }
            println(">>> continue to search for correct method")
          }
        }
      }
      result
      } else {
        throw new AdataoException(AdataoExceptionCode.ERR_GENERAL,
              s"Cannot find method $methodName from command ${commandChain.mkString(".")}", null)
      }
  }
}

