/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 *
 */
package com.adatao.pa.spark.types

import scala.Array.canBuildFrom
import scala.annotation.implicitNotFound
import com.google.gson.ExclusionStrategy
import com.google.gson.FieldAttributes
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.adatao.ML.TCanLog
import com.adatao.ML.TModel
import com.adatao.ML.types.TJsonSerializable
import com.adatao.ML.types.TJsonSerializable
import java.util.Arrays

/**
 * Result object to be returned from TExecutor.run().
 *
 * @param success
 * @param result
 * @param persistenceID - optional, provides persistence ID of the result object if persisted server-side
 * @param _resultTypeManifest - we precede this with an underscore _ because we don't want it to be ser/des automatically; it's not useful in that form for JSON clients anyway
 */
class ExecutionResult[ResultType](val success: Boolean, var result: ResultType, var persistenceID: String = null) extends Serializable {
	def isSuccess = success
	private val resultType = Option(result).getOrElse(None).getClass.getName // include this information in case clients want to do strict type checking of the JSON
	def toJson: String = ExecutionResult.toJson(this)
}

object ExecutionResult {
	/**
	 * Use this signature for backward-compatibility with old [[ExecutorResult]] [[CExecutors]]
	 */
	def newInstance(xr: ExecutorResult) = {
		xr match {
			case sr: SuccessResult ⇒ new SuccessfulResult(sr)
			case fr: FailResult ⇒ new FailedResult(fr.message, fr)
			case _ ⇒ new FailedResult("Unknown result type", xr)
		}
	}

	/**
	 * Deserializes the given json string into an [[ExecutionResult]] wrapping an object of the given type T.
	 *
	 * @param json - the JSON string to be deserialized
	 * @return an object of type ExecutionResult[T]
	 */
	def fromJson[T](json: String)(implicit m: Manifest[T]): ExecutionResult[T] = getDeserializer(m.erasure).fromJson(json, m.erasure)

	/**
	 * Deserializes the given json string into an [[ExecutionResult]] wrapping an object of the given [[java.lang.Class]] cls.
	 *
	 * Programming note: Notice how the parameterized type T is inferred from the innerClass variable. This is an important
	 * Java-Scala generic bridge technique.
	 *
	 * @param json - the JSON string to be deserialized
	 * @param innerClass - the (Java) class of the inner result
	 */
	def fromJson[T](json: String, innerClass: Class[T]): ExecutionResult[T] = getDeserializer(innerClass).fromJson(json, innerClass)

	/**
	 * Serializes the given [[ExecutionResult]] obj into a JSON string
	 */
	def toJson[T](obj: ExecutionResult[T]) = ExecutionResult.serializer.toJson(obj)

	/**
	 * Returns a ready-made [[Gson]] object that knows how to deserialize an [[ExecutionResult[innerClass]]] properly
	 */
	private def getDeserializer(innerClass: Class[_]): Gson = new GsonBuilder()
		.registerTypeAdapter(innerClass, new ExecutionResultDeserializer)
		.create

	/**
	 * We need our own Gson to serialize ourselves and embedded objects using their particular rules,
	 * deferring to their toJson() accordingly.
	 */
	private val serializer: Gson = TJsonSerializable.getGsonBuilder
		.serializeSpecialFloatingPointValues
		.setExclusionStrategies(new ExclusionStrategy() {
			// Don't serialize fields with names like "LOG", "_gson", etc. because they don't need to be
			def shouldSkipField(f: FieldAttributes): Boolean = {
				val name = f.getName
				return (
					// var names starting with _ will not be serialized (see getGson setExeclusionStrategies)
					name.startsWith("_") ||
					// also don't bother serializing LOG objects
					name.equals("LOG")
				)
			}

			def shouldSkipClass(clazz: Class[_]): Boolean = false
		})
		.create
}

/**
 * Derive from this class to signal a successful result
 */
class SuccessfulResult[ResultType](result: ResultType) extends ExecutionResult[ResultType](true, result) {
}

/**
 * Derive from this class to signal a failed result
 */
class FailedResult[ResultType](val message: String, result: ResultType = null.asInstanceOf[ResultType]) extends ExecutionResult[ResultType](false, result) {
	def this(message: String) = this(message, null.asInstanceOf[ResultType]) // For Java compat entry point
}

object FailedResult {
	def apply(message: String) = new FailedResult(message, null.asInstanceOf[Any])
	def apply(message: String, result: Any) = new FailedResult(message, result)

	/**
	 * Use this signature for instantiating from Java
	 */
	def newInstance(e: Exception) = new FailedResult(e.getMessage(), e)
}

/**
 * Provide our own deserializer for ExecutionResult, so that we can correctly deserialize the result object
 * which is of some parameterized type.
 *
 * Note that there are two ways to get the type of the parameterized type: (1) as specified by the caller, or
 * (3) as specified by the JSON field "resultType". We've decided to use #1, because this allows us to have
 * strongly-typed compile-time checking.
 */
private class ExecutionResultDeserializer extends JsonDeserializer[ExecutionResult[_]] {
	private val _gson = TJsonSerializable.getGson

	/**
	 * Example JSON result:
	 * {{{
	 * 2013-07-27 02:08:13,001 [pool-4-thread-1] INFO  com.adatao.pa.thrift.RCommandsHandler -
	 * Returning result: JsonResult(
	 *   sid:c44bc99c-0de9-4118-a9f3-43bb8b1ed57f,
	 *   result:{
	 *     "success":true,
	 *     "result":{"weights":{"rows":2,"columns":1,"length":2,"data":[37.285,-5.344]},"trainingLosses":{"rows":2,"columns":1,"length":2,"data":[4.34878134468,4.34878134468]}},
	 *     "persistenceID":"1058251b-7c3b-4f20-a6f4-805170868a01",
	 *     "resultType":"com.adatao.ML.LinearRegressionModel"
	 *  }
	 * )
	 * }}}
	 */
	override def deserialize(jElem: JsonElement, theType: java.lang.reflect.Type, context: JsonDeserializationContext): ExecutionResult[Any] = {

		//		println("CTN >>> jElem is %s".format(jElem))
		jElem match {
			case executionResultObj: JsonObject ⇒ {

				// First deserialize the "result" field according to the specific type T
				val resultObj: Any = executionResultObj.get("result") match {
					case resultElem: JsonElement ⇒ {

						// Determine what the result's class should be
						val resultClass = Option(executionResultObj.get("resultType")) match {
							// JSON has embedded type specification in "resultType" field, so use that
							case Some(resultType) ⇒ Option(Class.forName(resultType.getAsString())) match {
								case Some(validClass) ⇒ validClass
								case None ⇒ theType
							}
							case None ⇒ theType
						}

						//						println("CTN >>> resultClass is %s".format(resultClass))
						_gson.fromJson(resultElem, resultClass)
					}
					case _ ⇒ null // no resultElem found, so no resultObj
				}

				// Now deserialize the whole ExecutionResult, without the pesky "result" field
				executionResultObj.remove("result")
				val execResult: ExecutionResult[Any] = executionResultObj.get("success").getAsString() match {
					case "true" ⇒ _gson.fromJson(jElem, classOf[SuccessfulResult[Any]])
					case _ ⇒ _gson.fromJson(jElem, classOf[FailedResult[Any]])
				}

				// And add that result object back in
				execResult.result = resultObj

				//				println("CTN >>> execResult is %s".format(execResult))
				execResult
			}
			case _ ⇒ new FailedResult[Any]("Result is not JsonObject: %s".format(jElem))
		}
	}
}

case class ExecutionException(message: String) extends Exception(message)
