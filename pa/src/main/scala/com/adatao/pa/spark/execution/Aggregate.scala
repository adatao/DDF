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
import com.adatao.pa.spark.{DataManager, SparkThread}
import com.adatao.pa.spark.DataManager.{SharkDataFrame, DataContainer}
import com.adatao.pa.spark.SparkThread
import com.adatao.pa.spark.types.{ExecutorResult, SuccessResult}
import com.adatao.pa.spark.execution.Aggregate._

import shark.api.JavaSharkContext
import scala.Array._
import scala.Serializable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.adatao.ML.types.TJsonSerializable

//import scala.collection.mutable.Map


import java.util.{Map => JMap}
import java.util

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 16/7/13
 * Time: 12:30 PM
 * To change this template use File | Settings | File Templates.
 */
/** Aggregate is equivalent to R aggregate
	*
	* @param dataContainerID
	* @param colnames: the columns in which the function will be calculate
	* @param groupBy: equivalent with sql queries "group by", separate the dataset into subset to be operated on by Func
	* @param func: the aggregate function to be calculated, right now only support "summary", "mean", "median", "variance"
	*/
class Aggregate(dataContainerID: String, colnames: Array[String], groupBy: Array[String], func: String) extends AExecutor[AggregateResult] {

	protected override def runImpl(context: ExecutionContext): AggregateResult = {
		val df = context.sparkThread.getDataManager().get(dataContainerID) match {
			case x: SharkDataFrame => x
			case _                 => throw new IllegalArgumentException("Only accept SharkDataFrame")
		}
		val sc: JavaSharkContext = context.sparkThread.getSparkContext().asInstanceOf[JavaSharkContext]
		val metaInfos = df.getMetaInfo
		colnames.foreach{
			x => {
				val idx= df.getColumnIndexByName(x)
				if(metaInfos(idx).getType == "string")
					throw new IllegalArgumentException("Don't support string type, column: " + x)
			}
		}
		
		var gB: String = ""
		groupBy foreach (x => gB = gB + String.format(" %s,", x))

		getResult(df.getTableName, sc, colnames, gB.substring(0, gB.length()-1), groupBy.length, func)
	}

	def getResult(tableName: String, sc: JavaSharkContext, colnames: Array[String],
								groupBy: String, numGroupByCols: Int, func: String): AggregateResult = func match {
		
		case "mean"     => {
			var cmd: String = ""
			colnames foreach (x => cmd = cmd + String.format(" avg(%s),", x))
			Aggregate.LOG.info("Command string= " + cmd)

			val res: List[Array[String]] = sc.sql("select" + cmd + groupBy + " from " + tableName + " group by " + groupBy).
				map(x => x.split('\t')).toList

			val mean = res.map(x => (x.takeRight(numGroupByCols).mkString(","), (colnames zip (x take colnames.length).map{m =>
        try{
          m.toDouble
        } catch{
          case e: NumberFormatException => Double.NaN
        }
      }).toMap.asJava)
			).toMap.asJava
			new MeanResult(mean)
		}
		case "median"   => {
			var cmd: String = ""
			colnames foreach (x => cmd = cmd + String.format(" percentile_approx(%s, 0.5),", x))
			Aggregate.LOG.info("Command string= " + cmd)

			val res: List[Array[String]] = sc.sql("select" + cmd + groupBy + " from " + tableName + " group by " + groupBy).
				map(x => x.split('\t')).toList
			val median = res.map(x => (x.takeRight(numGroupByCols).mkString(","), (colnames zip (x take colnames.length).map{
        m =>
          try{
            m.toDouble
          } catch {
            case e: NumberFormatException => Double.NaN
          }
        }).toMap.asJava)
			).toMap.asJava
			new MedianResult(median)
		}
		case "variance" => {
			var cmd: String = ""
			colnames foreach (x => cmd = cmd + String.format(" variance(%s),", x))
			Aggregate.LOG.info("Command string= " + cmd)

			val res: List[Array[String]] = sc.sql("select" + cmd + groupBy + " from " + tableName + " group by " + groupBy).
				map(x => x.split('\t')).toList
			val variance = res.map(x => (x.takeRight(numGroupByCols).mkString(","), (colnames zip (x take colnames.length).map{
        m =>
          try{
            m.toDouble
          } catch {
            case e: NumberFormatException => Double.NaN
          }
        }).toMap.asJava)
			).toMap.asJava
			new VarianceResult(variance)
		}
		case "sum" => {
			var cmd: String = ""
			colnames foreach (x => cmd = cmd + String.format(" sum(%s),", x))
			Aggregate.LOG.info("Command string= " + cmd)

			val res: List[Array[String]] = sc.sql("select" + cmd + groupBy + " from " + tableName + " group by " + groupBy).
				map(x => x.split('\t')).toList
			val sum = res.map(x => (x.takeRight(numGroupByCols).mkString(","), (colnames zip (x take colnames.length).map{
        m =>
         try{
           m.toDouble
         } catch {
           case e: NumberFormatException => Double.NaN
         }
        }).toMap.asJava)
			).toMap.asJava
			new SumResult(sum)
		}
		case "summary"  => {
			var cmd: String = ""
			var res: Map[String, JMap[String, AColumnSummaryResult]] = null
			for (x <- colnames) {
				cmd = String.format("select min(%s), max(%s)," +
					" percentile_approx(%s, array(0.25, 0.5, 0.75)), %s", x, x, x, groupBy)
				val resStr = sc.sql(cmd + " from " + tableName + " group by " + groupBy).toList
				val tmp1: List[Array[String]] = resStr.map(x => x.replace("[", "").replace("]", "").replace(",", "\t").
					split("\t"))
				val aColResult: Map[String, JMap[String, AColumnSummaryResult]] = tmp1.map(i =>
					(i.takeRight(numGroupByCols).mkString(","), Map(x -> new AColumnSummaryResult(i(0).toDouble,
						i(1).toDouble, i(3).toDouble,
						i(2).toDouble, i(4).toDouble)).asJava)).toMap
				//put aColResult to existing result
				res = res match {
					case null => aColResult
					case y    => y.keys.map(x => (x, (y(x) ++ aColResult(x)).asJava)).toMap
				}
			}
			Aggregate.LOG.info("Command string= " + cmd)
			new SummaryResult(res)
		}
		case _          => throw new IllegalArgumentException("Ony accept mean, median, variance, sum," +
																	" summary as aggregate function")
	}
}

object Aggregate{
	val LOG: Logger = LoggerFactory.getLogger(classOf[Aggregate])
  abstract class AggregateResult
	/* class AColumnSummaryResult contain result from aggregate summary for one column
	 *
	 */
	class AColumnSummaryResult(val min: Double, val max: Double,
														 val median: Double, val first_quartile: Double,
														 val third_quartile: Double) extends Serializable

	//ListResults: contain a Map of Map
	//The first map map from each level of group by,
	//the second contains columnName and correspond result
	class SummaryResult(val ListResults: JMap[String, JMap[String, AColumnSummaryResult]]) extends AggregateResult with TJsonSerializable{
    def this()= this(null)
  }

	class MeanResult(val ListResults: JMap[String, JMap[String, Double]]) extends AggregateResult with TJsonSerializable {
    def this()= this(null)
  }

	class MedianResult(val ListResults: JMap[String, JMap[String, Double]]) extends AggregateResult with TJsonSerializable {
    def this()= this(null)
  }

	class VarianceResult(val ListResults: JMap[String, JMap[String, Double]]) extends AggregateResult with TJsonSerializable {
    def this()= this(null)
  }

	class SumResult(val ListResults: JMap[String, JMap[String, Double]]) extends AggregateResult with TJsonSerializable {
    def this()= this(null)
  }
}
