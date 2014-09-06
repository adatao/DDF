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

package com.adatao.spark.ddf.analytics

import java.util.HashMap
import java.util.HashMap

import scala.collection.mutable.ListBuffer
import scala.util.Random

import io.ddf.types.Matrix
import io.ddf.types.Vector
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util

object Utils {

  // random initial weights (uniform distributed)
  def randWeights(numFeatures: Int) = Vector(Seq.fill(numFeatures)(Random.nextDouble).toArray)

  /**
   * Partition mapper to take an entire partition of many rows of Array[T], and map it into a Matrix X and a Vector Y
   * where the rows represent the data samples.
   *
   * This {@link RDD#mapPartitions(FlatMapFunction) mapPartitions()} is significantly faster than using
   * {@link RDD#map(Function) map()}, because we output only one data table object per partition.
   *
   * @author ctn
   *
   * @param yCol
   * @param xCols
   * @param inputRows
   * @return Iterator[(Matrix, Vector)]
   */

  def rowsToPartitionMapper(xCols: Array[Int], yCol: Int)(inputRows: Iterator[Array[Object]]): Iterator[(Matrix, Vector)] = {

    val rows = new ListBuffer[Array[Object]]
    var numRows = 0
    while (inputRows.hasNext) {
      val aRow = inputRows.next
      if (aRow != null) {
        rows.append(aRow)
        numRows += 1
      }
    }

    val numCols = xCols.length + 1 // 1 bias term + n x-features
    val Y = new Vector(numRows)
    val X = new Matrix(numRows, numCols)

    var row = 0
    rows.foreach(inputRow ⇒ {
      X.put(row, 0, 1.0) // bias term

      var i = 1
      while (i < numCols) {
        X.put(row, i, objectToDouble(inputRow(xCols(i - 1)))) // x-feature #i
        i += 1
      }

      Y.put(row, objectToDouble(inputRow(yCol))) // y-value
      row += 1
    })

    //		LOG.info("X is %s".format(X.toString))
    //		LOG.info("Y is %s".format(Y.toString))

    Iterator((X, Y))
  }

  /* Convert a numeric Object to Double,
	 * throwing exception if not numeric.
	 */
  def objectToDouble(o: Object): Double = o match {
    case i: java.lang.Integer => i.toDouble
    case f: java.lang.Float => f.toDouble
    case d: java.lang.Double => d
    case _ => throw new RuntimeException("not a numeric Object")
  }




  /**
   * Helps safely parse a String into a java Double.
   */
  def toJavaDouble(str: String): java.lang.Double = {
    try {
      java.lang.Double.parseDouble(str)
    } catch {
      case e : Throwable ⇒ Double.NaN
    }
  }

  /*
   * input: dataContainerID
   * output: DDF id
   */
  def dcID2DDFID(dataContainerID: String): String = {
    return dataContainerID;
  }
}

