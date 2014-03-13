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

package com.adatao.pa.spark.execution

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import com.adatao.pa.spark.DataManager.{DataFrame, SharkDataFrame, MetaInfo}
import com.adatao.pa.spark.{SharkUtils, SparkThread, DataManager}
import com.adatao.pa.spark.types.{ SuccessResult, ExecutorResult }
import scala.collection.JavaConversions._
import java.util.Map
import shark.api.JavaSharkContext
import com.google.gson.Gson
import com.adatao.ML.types.TJsonSerializable
import scala.annotation.tailrec

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 18/7/13
 * Time: 3:13 PM
 * To change this template use File | Settings | File Templates.
 */
/*
 * Return 5 summary statistics for SharkDataFrame:
 * min, max, median, 1st-quartile, 3rd-quartile for numerical columns
 * and factor count for factor columns
 */
class FiveNumSummary(dataContainerID: String) extends AExecutor[Array[ASummary]] {

	protected override def runImpl(context: ExecutionContext): Array[ASummary] = {
		val df = context.sparkThread.getDataManager.get(dataContainerID) match {
			case x: SharkDataFrame ⇒ x
			case _ ⇒ throw new IllegalArgumentException("Only accept SharkDataFrame")
		}
		val sc: JavaSharkContext = context.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
		getResult(df, sc)
	}

	def getResult(df: SharkDataFrame, sc: JavaSharkContext): Array[ASummary] = {
		val tableName = df.getTableName
		val metaInfo: List[MetaInfo] = df.getMetaInfo.toList
		var cmd = ""

		//helper function to put tmpArray[Double] into proper format for Array[Asummary]
		//plus NaN result for column that's not numeric
		@tailrec
		def helperFunction(result: Array[ASummary], tmpResult: Array[Double], metaInfos: List[MetaInfo]): Array[ASummary] = metaInfos match{
			case Nil => result
			case x :: xs => x.getType match {
				case "int"      => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case "double"   => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case "float"    => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case "bigint"   => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case "tinyint"  => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case "smallint" => helperFunction(result :+ new ASummary(tmpResult take 5),tmpResult drop 5, xs)
				case z: String  => helperFunction(result :+ new ASummary(),tmpResult, xs)
				case _          => throw new Exception("Error matching result")
			}
		}
		//only compute for non-String column
		for (x ← metaInfo) {
			val aType = x.getType
			if(aType == "int" || aType == "bigint" || aType == "smallint" || aType == "tinyint"){
				cmd = cmd + String.format(" percentile(%s, array(0, 1, 0.25, 0.5, 0.75)),", x.getHeader)
			}
			else if(aType == "double" || aType == "float"){
				cmd = cmd + String.format(" min(%s), max(%s), percentile_approx(%s, array(0.25, 0.5, 0.75)),",
					x.getHeader, x.getHeader, x.getHeader)
			}
		}
		//parse the last ","
		cmd = cmd take (cmd.length -1)
		val resultStr = sc.sql("select" + cmd + "from " + tableName)(0)
		LOG.info("resultStr = " + resultStr)
		val tmp:Array[Double] = resultStr.replace("[", "").replace("]", "").replace(",", "\t").split("\t").flatMap(x ⇒
			x match {
				//"null" is a result of percentile_approx(v, array(0.25, 0.75, 0.5))
				//so must expand it too NaN, NaN, NaN
				case "null" => List(Double.NaN, Double.NaN, Double.NaN)
				case "NULL" => List(Double.NaN)
				case a =>	List(a.toDouble)
			}).toArray

		LOG.info("tmp.size = " + tmp.size)
		LOG.info("metaInfo.size = " + metaInfo.size)
		try{
			helperFunction(Array[ASummary](), tmp, metaInfo)
		}
		catch{
			case e => throw new Exception("Error parsing result: Array[Double]= " + tmp.mkString(","))
		}
	}
}

object FiveNumSummary extends Serializable {
	/**
	 * [[ASummary]] extends [[TJsonSerializable]] so that when we have an array of [[ASummary]], each member
	 * will have a "clazz" field describing its exact concrete type.
	 */
	//FiveNumSummary only support Numeric column
	class ASummary(
		val min: Double = Double.NaN,
		val max: Double = Double.NaN,
		val first_quartile: Double = Double.NaN,
		val median: Double = Double.NaN,
		val third_quartile: Double = Double.NaN) extends TJsonSerializable {
		def this() = this(Double.NaN)
		def this(a: Array[Double]) = this(a(0), a(1), a(2), a(3), a(4))
	}
}
