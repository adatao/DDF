package com.adatao.pa.spark.execution

import java.lang.reflect.{Field, Method}
import  org.junit.Assert.assertEquals;
import  org.junit.Assert.assertFalse;
import  org.junit.Assert.assertTrue;

import adatao.ML.ATestSuite
 import com.adatao.pa.spark.execution.SubsetFilterSuite._
import com.adatao.pa.spark.execution.Subset.{ExprDeserializer, FilterMapper}
;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder
import com.adatao.pa.spark.types.ABigRClientTest
import org.scalatest.FunSuite
;
/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 22/10/13
 * Time: 9:50 PM
 * To change this template use File | Settings | File Templates.
 */

// @formatter:off
class SubsetFilterSuite extends ATestSuite{
	test("test BinOp"){

		val gsonBld= new GsonBuilder()
		gsonBld.registerTypeAdapter(classOf[Subset.Expr], new ExprDeserializer)

		val gson: Gson = gsonBld.create()

		val inp =   Array(10.asInstanceOf[AnyRef])

		var json = "{filter: {type: Operator, " + "name: lt, " + "operands: [{type: Column, index: 0}," +
			 "{type: IntVal, value: 12}]" + "}" + "}"
		var gv: Subset = gson.fromJson(json, classOf[Subset])
		var f: FilterMapper = new FilterMapper(gv.getFilter)

		inp(0) = 1.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call", classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call", classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call", classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: le, " + "operands: [{type: Column, index: 0}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter)

		inp(0) = 1.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: eq, " + "operands: [{type: Column, index: 0}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter());

		inp(0) = 1.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: ge, " + "operands: [{type: Column, index: 0}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = 1.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: gt, " + "operands: [{type: Column, index: 0}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = 1.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: ne, " + "operands: [{type: Column, index: 0}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = 1.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: ne, " + "operands: [{type: Column, index: 0}," +
			"{type: DoubleVal, value: 12.0}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = 1.0.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 12.0.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = 14.0.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: eq, " + "operands: [{type: Column, index: 0}," +
			"{type: StringVal, value: abcd}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = "abcd".asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = "abcdasd".asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = "bcd".asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, " + "name: eq, " + "operands: [{type: Column, index: 0}," +
			"{type: BooleanVal, value: true}]" + "}" + "}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = true.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
		inp(0) = false.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
	}

	test("test LogicOp") {

		val gsonBld= new GsonBuilder
		gsonBld.registerTypeAdapter(classOf[Subset.Expr], new ExprDeserializer)
		val gson = gsonBld.create()

		var json = "{filter: {type: Operator, name: and, " + "operands: [{type: Operator, " + "name: le, " +
		 "operands: [{type: Column, index: 0}," + "{type: IntVal, value: 12}]" + "}," + "{type: Operator, " +
		 "name: gt, " + "operands: [{type: Column, index: 1}," + "{type: IntVal, value: 48}]" + "}]" + "}" +
		 "}"
		var gv = gson.fromJson(json, classOf[Subset])
		var f = new FilterMapper(gv.getFilter())
		val inp =   Array(1.asInstanceOf[AnyRef], 100.asInstanceOf[AnyRef])

		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		inp(0) = 1.asInstanceOf[AnyRef]
		inp(1) = 48.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call", classOf[Array[Object]], inp).asInstanceOf[Boolean])

		json = "{filter: {type: Operator, name: or, " + "operands: [{type: Operator, " + "name: le, " +
			"operands: [{type: Column, index: 0}," + "{type: IntVal, value: 12}]" + "}," + "{type: Operator, " +
			"name: gt, " + "operands: [{type: Column, index: 1}," + "{type: IntVal, value: 48}]" + "}]" + "}" +
			"}"
		gv = gson.fromJson(json, classOf[Subset])
		f = new FilterMapper(gv.getFilter())

		inp(0) = 1.asInstanceOf[AnyRef]
		inp(1) = 100.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		inp(0) = 1.asInstanceOf[AnyRef]
		inp(1)= 48.asInstanceOf[AnyRef]
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		inp(0) = 15.asInstanceOf[AnyRef]
		inp(1)= 45.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
	}

	test("Test Multiple Logic Op"){
		val gsonBld= new GsonBuilder
		gsonBld.registerTypeAdapter(classOf[Subset.Expr], new ExprDeserializer)
		val gson = gsonBld.create()

		var json = "{filter: {type: Operator, " + "				name: and, " + "operands: [{type: Operator, " +
			"name: le, " + "operands: [{type: Column, index: 0}," + "{type: IntVal, value: 12}]},"+
			"{type: Operator, " + "name: or, " + "operands: [{type: Operator, " + "name: eq, " +
			"operands: [{type: Column, index: 1}," + "{type: IntVal, value: 50}]}," + "{type: Operator, " +
			"name: gt, " + "operands: [{type: Column, index: 2}," + "{type: IntVal, value: 48}] }] }]}}"
		var gv = gson.fromJson(json, classOf[Subset])
		var f = new FilterMapper(gv.getFilter())
		val inp =   Array(1.asInstanceOf[AnyRef], 50.asInstanceOf[AnyRef], 3.asInstanceOf[AnyRef])
		assertTrue(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])

		inp(0) = 1.asInstanceOf[AnyRef]
		inp(1)= 34.asInstanceOf[AnyRef]
		inp(2)= 34.asInstanceOf[AnyRef]
		assertFalse(invokeFunction(f, "call",classOf[Array[Object]], inp).asInstanceOf[Boolean])
	}
	test("Test Sql Construction"){
		var json = "{filter: {type: Operator, " + "name: lt, " + "operands: [{type: Column, name: foo}," +
			 "{type: IntVal, value: 12}]" + "}" + "}"
		assertFilterExprEquals("(foo < 12)", json)

		json = "{filter: {type: Operator, " + "name: le, " + "operands: [{type: Column, name: bar}," +
			 "{type: IntVal, value: 12}]" + "}" + "}"
		assertFilterExprEquals("(bar <= 12)", json)

		json = "{filter: {type: Operator, " + "name: eq, " + "operands: [{type: Column, name: x}," +
			"{type: IntVal, value: 12}]" + "}" + "}"
		assertFilterExprEquals("(x == 12)", json)

		json = "{filter: {type: Operator, " +
			"name: and, " +
			"operands: [" +
			"{type: Operator, " + "name: le, " + "operands: [{type: Column, name: f1}, {type: IntVal, value: 12}]}," +
			"{type: Operator, name: gt, " + "operands: [{type: Column, name: f2}," + "{type: IntVal, value: 48}]" + "}]" + "}" +
			"}";
		assertFilterExprEquals("((f1 <= 12) AND (f2 > 48))", json)

		json = "{filter: {type: Operator, " +
			"name: neg, " +
			"operands: [" +
			"{type: Operator, " + "name: eq, operands: [{type: Column, name: f1}, {type: IntVal, value: 13}]}" +
			"]}}";
		assertFilterExprEquals("(NOT (f1 == 13))", json)

		// nested logical operators
		json = "{filter: {type: Operator, " +
			"name: and, " +
			"operands: [" +
			"{type: Operator, name: ge, operands: [{type: Column, name: y}, {type: DoubleVal, value: 12.5}]}," +
			"{type: Operator, name: or," +
			"operands: [" +
			"{type: Operator, name: gt, operands: [{type: Column, name: x1}, {type: IntVal, value: 50}]}," +
			"{type: Operator, name: eq, operands: [{type: Column, name: x2}, {type: StringVal, value: \"barz\"}]}" +
			"]}]}}";
		assertFilterExprEquals("((y >= 12.5) AND ((x1 > 50) OR (x2 == 'barz')))", json)

	}
}
object SubsetFilterSuite{

	def invokeFunction(obj: AnyRef, funcName: String, argClass: Class[_], arg: AnyRef)  = {

		val function: Method = obj.getClass().getDeclaredMethod(funcName, argClass)
		function.setAccessible(true)
		function.invoke(obj, arg)
	}

	def getField(obj: AnyRef, fieldName: String) = {
		val f: Field = obj.getClass.getDeclaredField(fieldName)
		f.setAccessible(true)
		f.get(obj)
	}

	def assertFilterExprEquals(sql: String, json: String) = {
		val gsonBld= new GsonBuilder
		gsonBld.registerTypeAdapter(classOf[Subset.Expr], new ExprDeserializer)

		val gson= gsonBld.create()
		val gv= gson.fromJson(json, classOf[Subset])
		val filter= getField(gv, "filter").asInstanceOf[Subset.Expr]
		assertEquals(sql, filter.toSql)

	}

}
