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

package adatao.bigr.spark.execution

import adatao.bigr.spark.DataManager.{ SharkColumnVector, DataFrame }
import shark.api.JavaSharkContext
import adatao.bigr.spark.types.NumericHistogram
import org.apache.spark.rdd.RDD
import java.util.regex.Pattern
import scala.collection.mutable.{ArrayBuffer, Map, HashMap}

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/7/13
 * Time: 12:56 PM
 * To change this template use File | Settings | File Templates.
 */
class VectorQuantiles(dataContainerID: String, pArray: Array[Double], B: Int = 10000) extends AExecutor[Array[Double]] {
	protected override def runImpl(context: ExecutionContext): Array[Double] = {
		val ret: Array[Double] = context.sparkThread.getDataManager.get(dataContainerID) match {
			case scv: SharkColumnVector ⇒ getQuantiles(scv, context.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext])
			case v: DataFrame ⇒ getDFQuantiles(v)
			case _ ⇒ throw new IllegalArgumentException("Only support SharkColumnVector or one-column DataFrame, a.k.a ColumnVector")
		}
		ret
	}

	def calPartialHistogram(inputRows: Iterator[Array[Object]]): Iterator[NumericHistogram] = {
		val ret: NumericHistogram = new NumericHistogram()
		ret.allocate(B)
		while (inputRows.hasNext) {
			// A Vector is a DataFrame with only one column
			ret.add(inputRows.next()(0).asInstanceOf[Double])
		}
		Iterator(ret)
	}

	def calPartialMinMax(inputRows: Iterator[Array[Object]]): Iterator[(Double, Double)] = {
		var min = Double.MaxValue
		var max = Double.MinValue
		while (inputRows.hasNext) {
			val x = inputRows.next().apply(0).asInstanceOf[Double]
			if (x < min)
				min = x
			if (x > max)
				max = x
		}
		Iterator((min, max))
	}

	def getDFQuantiles(df: DataFrame): Array[Double] = {
		val rdd = df.getRDD().rdd
		val histogram_approx = rdd.mapPartitions(calPartialHistogram).reduce((x, y) ⇒ { x.merge(y.serialize()); x })

		// check if user want to calculate 0 and 1 quantiles which correspond to min and max values
		var minmax = false
		var i = 0
		while (i < pArray.length) {
			if (pArray(i) == 0 || pArray(i) == 1)
				minmax = true
			i += 1	
		}
		
		var min_max: (Double, Double) = null
		if (minmax) {

			val dfs = df.getQuickSummary();

			if (dfs == null) {
				// Calculate min, max value
				min_max = rdd.mapPartitions(calPartialMinMax).reduce {
					(x, y) ⇒
						{
							var min = x._1
							var max = x._2
							if (x._1 > y._1) {
								min = y._1
							}
							if (x._2 < y._2) {
								max = y._2
							}
							(min, max)
						}
				}
			}
			else {
				min_max = (dfs.min.asInstanceOf[Double], dfs.max.asInstanceOf[Double])
			}
		}

		pArray.map(p ⇒ p match {
			case 0 ⇒ min_max._1
			case 1 ⇒ min_max._2
			case p ⇒ histogram_approx.quantile(p)
		})
	}

	//hive UDF doesn't allow percentile_approx for 0.0 and 1.0
	//so we have to send separate command for min(), max()
	//and concatenate result
	def getQuantiles(df: SharkColumnVector, sc: JavaSharkContext): Array[Double] = {
		var pValues = pArray.toList.distinct.sortWith(_ < _)
		LOG.info("pValues = {}", pValues)
		var hasZero = false
		var hasOne = false
		var params = ""
		val Pattern1 = """^[big|small|tiny]{0,1}int$""".r
		val Pattern2 = """^float|double$""".r
		val colType = df.getColumnMetaInfoByName(df.getColumn()).getType()

		var (min, max) = ("", "")
		if (pValues(0) == 0) {
		  min = "min(" + df.getColumn() + ")"
		  pValues = pValues.takeRight(pValues.length-1)
		  hasZero = true
		}
		if (pValues(pValues.length-1) == 1) {
		  max = "max(" + df.getColumn() + ")"
		  pValues = pValues.take(pValues.length-1)
		  hasOne = true
		}

		var pParams = ""
		if (pValues.length > 0) {
			if (Pattern1.pattern.matcher(colType).matches)
				pParams = "percentile(" + df.getColumn() + ", array" + pValues.toString.replace("[", "(").replace(")", ")").replace("List", "") + ")"
			else if (Pattern2.pattern.matcher(colType).matches)
			  pParams = "percentile_approx(" + df.getColumn() + ", array" + pValues.toString.replace("[", "(").replace(")", ")").replace("List", "") + ", " + B.toString + ")"
			else 
			  throw new IllegalArgumentException("Only support numeric vectors!!!")
		}
		LOG.info("pParams = {}", pParams)

		var qmm = new ArrayBuffer[String]
		if (pParams.length > 0) qmm += pParams
		if (min.length > 0) qmm += min
		if (max.length > 0) qmm += max

		val cmd = "select " + qmm.mkString(", ") + " from " + df.getTableName
		LOG.info("Command String = " + cmd)
		val wholeRes = sc.sql(cmd)
		val result = wholeRes.get(0).replace("[", "").replace("]", "").replace(",", "\t").split("\t").map(x ⇒ x.toDouble)
		LOG.info("result = " + result.length + " pValues = " + pValues.length)
		val mapValues : Map[Double, Double] = new HashMap[Double, Double]
		for (i <- 0 to pValues.length-1)
		  mapValues.put(pValues(i), result(i))
		if (hasZero) 
		  mapValues.put(0, result(result.length-2))
		if (hasOne) 
		  mapValues.put(1, result(result.length-1))
		
		pArray.map(p => mapValues.get(p).get.doubleValue)
	}
}
