package com.adatao.spark.ddf.analytics

import com.adatao.ddf.DDF
import scala.collection.JavaConversions._
import com.adatao.ddf.analytics.IHandleBinning
import com.adatao.ddf.content.Schema.Column
import java.text.DecimalFormat
import scala.annotation.tailrec

class BinningHandler(mDDF: DDF) extends IHandleBinning(mDDF) {

    def getIntervalsFromNumBins(col: Column, bins: Int) = {
      val cmd = "SELECT min(%s), max(%s) FROM %s".format(col.getName, col.getName, mDDF.getTableName)
      val res: Array[Double] = mDDF.sql2txt(cmd,"").get(0).split("\t").map(x => x.toDouble)
      val (min, max) = (res(0), res(1))
      val eachInterval = (max - min) / bins
      val pArray: Array[Double] = Array.fill[Double](bins + 1)(0)
      var i = 0
      while (i < bins + 1) {
        pArray(i) = min + i * eachInterval
        i += 1
      }
      pArray(bins) = max
      pArray
    }

    def getQuantilesFromNumBins(col: Column, bins: Int): Array[Double] = {
      val eachInterval = 1.0 / bins
      val pArray: Array[Double] = Array.fill[Double](bins - 1)(0.0)
      var i = 0
      while (i < bins - 1) {
        pArray(i) = (i + 1) * eachInterval
        i += 1
      }
      getQuantiles(col, pArray)
    }

    /**
     * using hive UDF to get percentiles as breaks
     *
     */
    def getQuantiles(col: Column, pArray: Array[Double]): Array[Double] = {
      var cmd = ""
      pArray.foreach(x => cmd = cmd + x.toString + ",")
      cmd = cmd.take(cmd.length - 1)
      cmd = String.format("min(%s), percentile_approx(%s, array(%s)), max(%s)", col.getName, col.getName, cmd, col.getName)
      mDDF.sql2txt("SELECT " + cmd + String.format(" FROM %s", mDDF.getTableName),"").get(0).replace("[", "").
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
      private def createIntervals(result: Array[(Double => Boolean, String)], stopping: List[Double], first: Boolean): Array[(Double => Boolean, String)] = stopping match {
        case Nil => result
        case x :: Nil => result
        case x :: y :: xs => {
          if (includeLowest && right)
            if (first)
              createIntervals(result :+ ((z: Double) => z >= x && z <= y, "[" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)
            else
              createIntervals(result :+ ((z: Double) => z > x && z <= y, "(" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)

          else if (includeLowest && !right)
            if (xs == Nil)
              createIntervals(result :+ ((z: Double) => z >= x && z <= y, "[" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)
            else
              createIntervals(result :+ ((z: Double) => z >= x && z < y, "[" + formatter.format(x) + "," + formatter.format(y) + ")"), y :: xs, false)

          else if (!includeLowest && right)
            createIntervals(result :+ ((z: Double) => z > x && z <= y, "(" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)

          else
            createIntervals(result :+ ((z: Double) => z >= x && z < y, "[" + formatter.format(x) + "," + formatter.format(y) + ")"), y :: xs, false)
        }
      }
      def findInterval(aNum: Double): Option[String] = {
        intervals.find { case (f, y) => f(aNum) } match {
          case Some((x, y)) => Option(y)
          case None => Option(null)
        }
      }
    }
  
}