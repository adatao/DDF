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

package com.adatao.ML.spark

import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import com.adatao.spark.RDDImplicits._
import org.apache.spark.rdd.RDD

// @author: aht

object RddUtils {

	// calculate the mean value of the distributed Vector y from a RDD[(Matrix, Vector)]
	def yMean(xyRdd: RDD[(Matrix, Vector)], defaultIfEmpty: Double = Double.NaN): Double = {
		val tupSumCount = xyRdd.map(xy ⇒ xy match {
			case (x, y) ⇒
				(y.columnSum, y.rows) // sum and count tuple
		}).safeReduce((a, b) ⇒ (a._1 + b._1, a._2 + b._2), (Double.NaN, 1))

		if (tupSumCount._1.isNaN) defaultIfEmpty else tupSumCount._1 / tupSumCount._2
	}
}
