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
import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.{ SparkThread, DataManager }
import com.adatao.pa.spark.types.{ SuccessResult, ExecutorResult }
import scala.collection.JavaConversions._
import java.util.Map
import shark.api.JavaSharkContext
import com.google.gson.Gson
import com.adatao.ML.types.TJsonSerializable
import scala.annotation.tailrec
import com.adatao.ddf.DDF
import com.adatao.ML.Utils
import com.adatao.ddf.DDF

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
    val ddfManager = context.sparkThread.getDDFManager();
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfId) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }
    val fiveNums = ddf.getFiveNumSummary
    val fiveNumsResult = for (s <- fiveNums) yield new ASummary(s.getMin, s.getMax, s.getFirst_quantile, s.getMedian, s.getThird_quantile)
    return fiveNumsResult
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
