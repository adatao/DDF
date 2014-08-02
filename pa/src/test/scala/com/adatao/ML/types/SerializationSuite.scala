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

package com.adatao.ML.types

import com.adatao.ML.ATestSuite
import com.google.gson.GsonBuilder
import com.google.gson.Gson
import com.adatao.ML.ATimedAlgorithmTest
import com.adatao.ML.AAlgorithmTest
import com.adatao.spark.ddf.analytics.LinearRegressionModel

private class AJsonSerializable extends TJsonSerializable {
	val aField = "someValue"
}

class SerializationSuite extends ATestSuite {
	private val vector = Vector(1, 2, 3, 4, 5)
//	private val jsonString = vector.toJson
	private val regularGson = new GsonBuilder().create
	private val specialGson = TJsonSerializable.getGson

//	test("No SOE in serdes of TJsonSerializable") {
//		val testValue = new AJsonSerializable
//		val testJson = testValue.toJson
//		val testValue2 = TJsonSerializable.fromJson(testJson, classOf[AJsonSerializable])
//		//		println("CTN >>> testValue = " + testValue)
//		//		println("CTN >>> testJson = " + testJson)
//		//		println("CTN >>> testValue2 = " + testValue2)
//		assert(testValue2.aField === "someValue")
//	}
//
//	test("Vector serialization works") {
//		assert(jsonString === "[1.0,2.0,3.0,4.0,5.0]")
//	}
//
//	test("Vector deserialization works") {
//		assert(vector === Vector.fromJson(jsonString))
//	}
//
//	test("Can serialize/deserialize java.lang.{Float,Double} infinities and NaNs") {
//		assert(specialGson.toJson(Float.PositiveInfinity.asInstanceOf[java.lang.Float]) === "\"Infinity\"")
//		assert(specialGson.toJson(Float.NegativeInfinity.asInstanceOf[java.lang.Float]) === "\"-Infinity\"")
//		assert(specialGson.toJson(Float.NaN.asInstanceOf[java.lang.Float]) === "null")
//
//		assert(specialGson.fromJson("Infinity", classOf[java.lang.Float]) === Float.PositiveInfinity)
//		assert(specialGson.fromJson("-Infinity", classOf[java.lang.Float]) === Float.NegativeInfinity)
//		assert(specialGson.fromJson("NaN", classOf[java.lang.Float]).isNaN)
//
//		assert(specialGson.toJson(Double.PositiveInfinity.asInstanceOf[java.lang.Double]) === "\"Infinity\"")
//		assert(specialGson.toJson(Double.NegativeInfinity.asInstanceOf[java.lang.Double]) === "\"-Infinity\"")
//		assert(specialGson.toJson(Double.NaN.asInstanceOf[java.lang.Double]) === "null")
//
//		assert(specialGson.fromJson("Infinity", classOf[java.lang.Double]) === Double.PositiveInfinity)
//		assert(specialGson.fromJson("-Infinity", classOf[java.lang.Double]) === Double.NegativeInfinity)
//		assert(specialGson.fromJson("NaN", classOf[java.lang.Double]).isNaN)
//	}
//
//	test("Can serialize tuples") {
//		assert(specialGson.toJson((1, 1.2)) ===
//			"{\"tuple\":[1,1.2],\"types\":[\"java.lang.Integer\",\"java.lang.Double\"]}")
//
//		assert(specialGson.toJson((1, "two")) ===
//			"{\"tuple\":[1,\"two\"],\"types\":[\"java.lang.Integer\",\"java.lang.String\"]}")
//
//		assert(specialGson.toJson((1, "two", 3)) ===
//			"{\"tuple\":[1,\"two\",3],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"java.lang.Integer\"]}")
//
//		assert(specialGson.toJson((1, "two", (31, "three.point.two"))) ===
//			"{\"tuple\":[1,\"two\",{\"tuple\":[31,\"three.point.two\"],\"types\":[\"java.lang.Integer\",\"java.lang.String\"]}],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"scala.Tuple2\"]}")
//
//		assert(specialGson.toJson((1, 2, 3, 4, 5, 6, 7, 8, 9)) ===
//			"{\"tuple\":[1,2,3,4,5,6,7,8,9],\"types\":[\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\"]}")
//
//		assert(specialGson.toJson((1, "two", new LinearRegressionModel(weights = Vector(1, 2), trainingLosses = Vector(0, 0), 10))) ===
//			"{\"tuple\":[1,\"two\",{\"trainingLosses\":[0.0,0.0],\"weights\":[1.0,2.0],\"numSamples\":10,\"dummyColumnMapping\":{},\"mapReferenceLevel\":{}}],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"com.adatao.ML.LinearRegressionModel\"]}")
//	}
//
//	test("Can deserialize tuples") {
//		assert(((1, 1.2)) ===
//			specialGson.fromJson("{\"tuple\":[1,1.2],\"types\":[\"java.lang.Integer\",\"java.lang.Double\"]}", classOf[Tuple2[_, _]]))
//
//		assert(((1, "two")) ===
//			specialGson.fromJson("{\"tuple\":[1,\"two\"],\"types\":[\"java.lang.Integer\",\"java.lang.String\"]}", classOf[Tuple2[_, _]]))
//
//		assert(((1, "two", 3)) ===
//			specialGson.fromJson("{\"tuple\":[1,\"two\",3],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"java.lang.Integer\"]}", classOf[Tuple3[_, _, _]]))
//
//		assert(((1, "two", (31, "three.point.two"))) ===
//			specialGson.fromJson("{\"tuple\":[1,\"two\",{\"tuple\":[31,\"three.point.two\"],\"types\":[\"java.lang.Integer\",\"java.lang.String\"]}],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"scala.Tuple2\"]}",
//				classOf[Tuple3[_, _, _]]))
//
//		assert(((1, 2, 3, 4, 5, 6, 7, 8, 9)) ===
//			specialGson.fromJson("{\"tuple\":[1,2,3,4,5,6,7,8,9],\"types\":[\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\",\"java.lang.Integer\"]}",
//				classOf[Tuple9[_, _, _, _, _, _, _, _, _]]))
//
//		val a = ((1, "two", new LinearRegressionModel(weights = Vector(1, 2), trainingLosses = Vector(0, 0), numSamples= 10)))
//		val b = specialGson.fromJson("{\"tuple\":[1,\"two\",{\"weights\":[1.0,2.0],\"trainingLosses\":[0.0,0.0]}],\"types\":[\"java.lang.Integer\",\"java.lang.String\",\"com.adatao.ML.LinearRegressionModel\"]}",
//			classOf[Tuple3[_, _, _]])
//		assert(a._1 === b._1)
//		assert(a._2 === b._2)
//		assert(a._3.weights === b._3.asInstanceOf[LinearRegressionModel].weights)
//		assert(a._3.trainingLosses === b._3.asInstanceOf[LinearRegressionModel].trainingLosses)
//	}

}
