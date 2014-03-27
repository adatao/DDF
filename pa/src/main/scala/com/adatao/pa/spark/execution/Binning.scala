package com.adatao.pa.spark.execution

import shark.api.{JavaSharkContext, Row}
import com.adatao.pa.spark.DataManager.{SharkColumnVector, SharkDataFrame, MetaInfo}
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.annotation.tailrec
import com.adatao.pa.spark.execution.Binning.{getIntervalsFromNumBins, getQuantilesFromNumBins, ColReducer, Intervals, ColMapper, MAX_LEVEL_SIZE}
import java.text.DecimalFormat
import com.adatao.pa.spark.Utils
import com.adatao.ddf.DDF

class BinningResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])

/**
 *
 * @param dataContainerID
 * @param col: column name to do binning
 * @param binningType: support 3 binning types "custom", "equalFreq", "equalInterval"
 * @param numBins: number of bins
 * @param breaks: Array of points to specify the intervals, used for binning type "equalFred" and "equalInterval"
 * @param includeLowest: see R cut function
 * @param right:  see R cut function
 * @param decimalPlaces: number of decimal places in range format
 */
class Binning(val dataContainerID: String,
							val col: String,
							val binningType: String,
							val numBins: Int = 0,
							var breaks: Array[Double] = null,
							val includeLowest: Boolean = false,
							val right: Boolean = true,
							val decimalPlaces: Int = 2) extends AExecutor[BinningResult]{

	protected override def runImpl(context: ExecutionContext): BinningResult = {
	  
	  val ddf = context.sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
	  val newddf = ddf.binning(col, binningType, numBins, breaks, includeLowest, right)
	  // binned var are now factors
    //new GetFactor().setDataContainerID(Utils.getDataContainerId(newddf)).setColumnName(col).run(context.sparkThread)
	  new BinningResult(Utils.getDataContainerId(newddf), Utils.generateMetaInfo(newddf.getSchema()))
	}

}

object Binning {

	def getIntervalsFromNumBins(df: SharkDataFrame, sc: JavaSharkContext, col: MetaInfo, bins: Int) = {
		val cmd = "select min(%s), max(%s) from %s".format(col.getHeader, col.getHeader, df.getTableName)
		val res: Array[Double] = sc.sql(cmd).get(0).split("\t").map(x => x.toDouble)
		val (min, max) = (res(0), res(1))
		val eachInterval = (max - min) / bins
		val pArray: Array[Double] = Array.fill[Double](bins + 1)(0)
		var i = 0
		while( i < bins + 1){
			pArray(i) = min + i * eachInterval
			i += 1
		}
		pArray(bins) = max
		pArray
	}

	def getQuantilesFromNumBins(df: SharkDataFrame, sc: JavaSharkContext, col: MetaInfo, bins: Int): Array[Double] ={
		val eachInterval = 1.0 / bins
		val pArray: Array[Double] = Array.fill[Double](bins - 1)(0.0)
		var i = 0
		while( i < bins - 1){
			pArray(i) = (i + 1) * eachInterval
			i += 1
		}
		getQuantiles(df, sc, col, pArray)
	}

	/**
	 * using hive UDF to get percentiles as breaks
	 *
	 */
	def getQuantiles(df: SharkDataFrame, sc: JavaSharkContext, col: MetaInfo, pArray: Array[Double]): Array[Double] = {
		var cmd = ""
		pArray.foreach(x => cmd = cmd + x.toString + ",")
		cmd = cmd.take(cmd.length - 1)
		cmd = String.format("min(%s), percentile_approx(%s, array(%s)), max(%s)", col.getHeader, col.getHeader, cmd, col.getHeader)
		sc.sql("select " + cmd + String.format(" from %s", df.getTableName)).get(0).replace("[", "").
			replace("]", "").replace(",", "\t").split("\t").map(x => x.toDouble)
	}

	val MAX_LEVEL_SIZE = Integer.parseInt(System.getProperty("factor.max.level.size", "1024"))

	/* Class to produce intervals from array of stopping
   * and method findInterval(Double) return an interval for a given Double
   */

	class Intervals(val stopping: List[Double], private val includeLowest: Boolean = false, right: Boolean = true,
									formatter: DecimalFormat) extends Serializable {
		val intervals = createIntervals(Array[(Double => Boolean, String)](), stopping, true)


		@tailrec
		private def createIntervals(result: Array[(Double => Boolean, String)], stopping: List[Double], first: Boolean):
								Array[(Double => Boolean, String)] = stopping match {
			case Nil          => result
			case x :: Nil     => result
			case x :: y :: xs => {
				if (includeLowest && right)
					if(first)
						createIntervals(result :+ ((z: Double) => z >= x && z <= y, "["+formatter.format(x)+"," + formatter.format(y) + "]"), y :: xs, false)
					else
						createIntervals(result :+ ((z: Double) => z >  x && z <= y, "("+formatter.format(x)+"," + formatter.format(y) +  "]"), y :: xs, false)

				else if (includeLowest && !right)
					if(xs == Nil)
						createIntervals(result :+ ((z: Double) => z >= x && z <= y, "["+formatter.format(x)+"," + formatter.format(y) + "]"), y :: xs, false)
					else
						createIntervals(result :+ ((z: Double) => z >= x && z < y, "["+formatter.format(x)+"," + formatter.format(y) + ")"), y :: xs, false)

				else if (!includeLowest && right)
					createIntervals(result :+ ((z: Double) => z > x && z <= y, "("+formatter.format(x)+"," + formatter.format(y) + "]"), y :: xs, false)

				else
					createIntervals(result :+ ((z: Double) => z >= x && z < y, "["+formatter.format(x)+"," + formatter.format(y) + ")"), y :: xs, false)
			}
		}
		def findInterval(aNum: Double): Option[String] = {
			intervals.find{case(f, y) => f(aNum) } match {
				case Some((x, y)) => Option(y)
				case None         => Option(null)
			}
		}
	}

	class ColMapper(colName: String, aType: String, intervals: Intervals)
		extends Function1[Iterator[Row], Iterator[Map[String, Int]]] with Serializable {

		@Override
		def apply(rowIter: Iterator[Row]): Iterator[Map[String, Int]] = {
			val aMap: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()
			val convertingFunction: Object => Double = aType match {
				case "int"    => x: Object => x.asInstanceOf[Int].toDouble
				case "double" => x: Object => x.asInstanceOf[Double]
				case "float"  => x: Object => x.asInstanceOf[Double]
				case "bigint" => x: Object => x.asInstanceOf[Long].toDouble
				case s 				=> throw new Exception("Not supporting type: " + s)
			}
			while(rowIter.hasNext){
				val row = rowIter.next
				Option(row(colName)) match {
					case Some(x) => {
						intervals.findInterval(convertingFunction(x)) match {
							case Some(string) => {
								aMap.get(string) match{
									case Some(num) => aMap(string) = num + 1
									case None      => aMap(string) = 1
								}
							}
							case None =>
						}
					}
					case None =>
				}
			}

			val immutableMap = Map(aMap.toSeq: _*)
			Iterator(immutableMap)
		}
	}

	class ColReducer extends Function2[Map[String, Int], Map[String, Int], Map[String, Int]]
		with Serializable {
		@Override
		def apply(aMap1: Map[String, Int], aMap2: Map[String, Int]): Map[String, Int] = {
			val newMap =  mutable.HashMap(aMap2.toSeq: _*)
			if(aMap1.size > MAX_LEVEL_SIZE)
				aMap1
			else if(aMap2.size > MAX_LEVEL_SIZE)
				aMap2
			else {
				aMap1.foreach{
					case (x ,y) => {
						aMap2.get(x) match {
							case Some(num) => newMap(x) = num + y
							case None      => newMap(x) = y
						}
					}
					case _ => throw new Exception("Error matching expression")
				}
				Map(newMap.toSeq: _*)
			}
		}
	}
}
