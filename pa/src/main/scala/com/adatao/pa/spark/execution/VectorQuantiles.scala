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


import com.adatao.pa.spark.DataManager.{ SharkColumnVector, DataFrame }
import shark.api.JavaSharkContext
import com.adatao.pa.spark.types.NumericHistogram
import org.apache.spark.rdd.RDD
import java.util.regex.Pattern
import scala.collection.mutable.{ ArrayBuffer, Map, HashMap }
import io.ddf.DDF

import com.adatao.ML.Utils

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/7/13
 * Time: 12:56 PM
 * To change this template use File | Settings | File Templates.
 */
class VectorQuantiles(dataContainerID: String, pArray: Array[Double], B: Int = 10000) extends AExecutor[Array[Double]] {
  protected override def runImpl(context: ExecutionContext): Array[Double] = {
    val ddfManager = context.sparkThread.getDDFManager();
    LOG.info("pValues = {}", pArray)
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfId) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException("Only accept DDF")
    }

    val pArray1: Array[java.lang.Double] = for (x <- pArray) yield x.asInstanceOf[java.lang.Double]
    val ret: Array[Double] = for (x <- ddf.getVectorQuantiles(pArray1)) yield x.asInstanceOf[Double]
    ret
  }
}
