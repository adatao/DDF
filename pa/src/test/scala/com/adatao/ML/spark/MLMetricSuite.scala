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

import org.scalatest.FunSuite
import com.adatao.spark.ddf.analytics.{ Utils ⇒ MLUtils, _ }
import io.ddf.types.Matrix
import io.ddf.types.Vector
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.pa.spark.execution.YtrueYpred
import io.ddf.ml.RocMetric

/**
 * Tests for model scoring metrics
 *
 * @author aht
 *
 */
@RunWith(classOf[JUnitRunner])
class MLMetricSuite extends FunSuite with SharedSparkContext {

	val mtcars = Array(
		Array(21.0, 6, 160.0, 110, 3.90, 2.620, 16.46, 0, 1, 4, 4),
		Array(21.0, 6, 160.0, 110, 3.90, 2.875, 17.02, 0, 1, 4, 4),
		Array(22.8, 4, 108.0, 93, 3.85, 2.320, 18.61, 1, 1, 4, 1),
		Array(21.4, 6, 258.0, 110, 3.08, 3.215, 19.44, 1, 0, 3, 1),
		Array(18.7, 8, 360.0, 175, 3.15, 3.440, 17.02, 0, 0, 3, 2),
		Array(18.1, 6, 225.0, 105, 2.76, 3.460, 20.22, 1, 0, 3, 1),
		Array(14.3, 8, 360.0, 245, 3.21, 3.570, 15.84, 0, 0, 3, 4),
		Array(24.4, 4, 146.7, 62, 3.69, 3.190, 20.00, 1, 0, 4, 2),
		Array(22.8, 4, 140.8, 95, 3.92, 3.150, 22.90, 1, 0, 4, 2),
		Array(19.2, 6, 167.6, 123, 3.92, 3.440, 18.30, 1, 0, 4, 4),
		Array(17.8, 6, 167.6, 123, 3.92, 3.440, 18.90, 1, 0, 4, 4),
		Array(16.4, 8, 275.8, 180, 3.07, 4.070, 17.40, 0, 0, 3, 3),
		Array(17.3, 8, 275.8, 180, 3.07, 3.730, 17.60, 0, 0, 3, 3),
		Array(15.2, 8, 275.8, 180, 3.07, 3.780, 18.00, 0, 0, 3, 3),
		Array(10.4, 8, 472.0, 205, 2.93, 5.250, 17.98, 0, 0, 3, 4),
		Array(10.4, 8, 460.0, 215, 3.00, 5.424, 17.82, 0, 0, 3, 4),
		Array(14.7, 8, 440.0, 230, 3.23, 5.345, 17.42, 0, 0, 3, 4),
		Array(32.4, 4, 78.7, 66, 4.08, 2.200, 19.47, 1, 1, 4, 1),
		Array(30.4, 4, 75.7, 52, 4.93, 1.615, 18.52, 1, 1, 4, 2),
		Array(33.9, 4, 71.1, 65, 4.22, 1.835, 19.90, 1, 1, 4, 1),
		Array(21.5, 4, 120.1, 97, 3.70, 2.465, 20.01, 1, 0, 3, 1),
		Array(15.5, 8, 318.0, 150, 2.76, 3.520, 16.87, 0, 0, 3, 2),
		Array(15.2, 8, 304.0, 150, 3.15, 3.435, 17.30, 0, 0, 3, 2),
		Array(13.3, 8, 350.0, 245, 3.73, 3.840, 15.41, 0, 0, 3, 4),
		Array(19.2, 8, 400.0, 175, 3.08, 3.845, 17.05, 0, 0, 3, 2),
		Array(27.3, 4, 79.0, 66, 4.08, 1.935, 18.90, 1, 1, 4, 1),
		Array(26.0, 4, 120.3, 91, 4.43, 2.140, 16.70, 0, 1, 5, 2),
		Array(30.4, 4, 95.1, 113, 3.77, 1.513, 16.90, 1, 1, 5, 2),
		Array(15.8, 8, 351.0, 264, 4.22, 3.170, 14.50, 0, 1, 5, 4),
		Array(19.7, 6, 145.0, 175, 3.62, 2.770, 15.50, 0, 1, 5, 6),
		Array(15.0, 8, 301.0, 335, 3.54, 3.570, 14.60, 0, 1, 5, 8),
		Array(21.4, 4, 121.0, 109, 4.11, 2.780, 18.60, 1, 1, 4, 2)
	).map { row => row.map {x => x.asInstanceOf[Object]} }

	test("RddUtils.yMean") {
		// mean(mtcars$mpg)
		val rdd = sc.parallelize(mtcars).mapPartitions(MLUtils.rowsToPartitionMapper(Array(3, 5), 0))
		assertEquals(20.09062, RddUtils.yMean(rdd), 0.01)
	}

	//run the following in R to compare result
	//pred <- prediction( ROCR.simple$predictions[1:10], ROCR.simple$labels[1:10])
	//perf <- performance(pred,"auc")
	//perf@y.values
	test("AUC score") {
		var pred: Array[Array[Double]] = Array(
			Array(0.0, 1, 1),
			Array(0.0, 1, 0.75),
			Array(0.0, 0.833333333333333, 0.75),
			Array(0.0, 0.6666667, 0.75),
			Array(0.0, 0.6666667, 0.5),
			Array(0.0, 0.6666667, 0.25),
			Array(0.0, 0.5, 0.25),
			Array(0.0, 0.333333333333333, 0.25),
			Array(0.0, 0.333333333333333, 0),
			Array(0.0, 0.166666666666667, 0),
			Array(0.2, 0, 0)
		)

		val roc = new RocMetric(pred, 0.0)
		roc.computeAUC
		roc.print
		assertEquals(roc.auc, 0.6666667, 0.0001)

		var pred2: Array[Array[Double]] = Array(
			Array(0.0, 1, 1),
			Array(0.2, 0, 0)
		)

		val roc2 = new RocMetric(pred2, 0.0)
		roc2.computeAUC
		roc2.print
		assertEquals(roc2.auc, 0.500, 0.0001)

		var pred3: Array[Array[Double]] = Array(
			Array(0.0, 1, 1),
			Array(0.2, 0.98, 0)
		)

		val roc3 = new RocMetric(pred3, 0.0)
		roc3.computeAUC
		roc3.print
		assertEquals(roc3.auc, 0.99, 0.0001)

	}
}
