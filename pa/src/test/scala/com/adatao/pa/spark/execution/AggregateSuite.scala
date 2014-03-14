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

import org.junit.Assert.{ assertEquals }

import com.adatao.pa.spark.execution.Aggregate.{ MeanResult, MedianResult, SummaryResult, VarianceResult, SumResult }

import scala.collection.JavaConversions._
import com.adatao.pa.spark.types.{ ABigRClientTest }

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 17/7/13
 * Time: 6:00 PM
 * To change this template use File | Settings | File Templates.
 */
class AggregateSuite extends ABigRClientTest {
	test("Test Aggregate") {

		createTableMtcars
		val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
		assert(df.isSuccess)
		val dcID = df.dataContainerID
		LOG.info("Got dataContainterID = " + dcID)

		val cmd1 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear"), "mean")
		val r1: MeanResult = bigRClient.execute[MeanResult](cmd1).result

		assertEquals(r1.ListResults("5")("mpg"), 21.38, 0.01)
		assertEquals(r1.ListResults("5")("disp"), 202.48, 0.01)
		assertEquals(r1.ListResults("4")("mpg"), 24.53, 0.01)
		assertEquals(r1.ListResults("4")("disp"), 123.01, 0.01)
		assertEquals(r1.ListResults("3")("mpg"), 16.10, 0.01)
		assertEquals(r1.ListResults("3")("disp"), 326.3, 0.01)

		val cmd2 = new Aggregate(dcID, Array("wt", "qsec"), Array("am"), "median")
		val r2: MedianResult = bigRClient.execute[MedianResult](cmd2).result
		assertEquals(r2.ListResults("1")("wt"), 2.26, 0.01)
		assertEquals(r2.ListResults("1")("qsec"), 16.96, 0.01)
		assertEquals(r2.ListResults("0")("wt"), 3.49, 0.01)
		assertEquals(r2.ListResults("0")("qsec"), 17.71, 0.01)

		val cmd3 = new Aggregate(dcID, Array("disp", "hp", "drat"), Array("carb"), "variance")
		val r3: VarianceResult = bigRClient.execute[VarianceResult](cmd3).result

		assertEquals(r3.ListResults("6")("disp"), 0.0, 0.01)
		assertEquals(r3.ListResults("1")("disp"), 4937.884, 0.01)
		assertEquals(r3.ListResults("2")("hp"), 1739.56, 0.01)
		assertEquals(r3.ListResults("1")("drat"), 0.263, 0.01)

		val cmd6 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear"), "sum")
		val r6: SumResult = bigRClient.execute[SumResult](cmd6).result

		assertEquals(r6.ListResults("5")("mpg"), 106.9, 0.01)
		assertEquals(r6.ListResults("5")("disp"), 1012.4, 0.01)
		assertEquals(r6.ListResults("3")("disp"), 4894.5, 0.01)

		val cmd4 = new Aggregate(dcID, Array("mpg", "disp"), Array("am"), "summary")
		val r4: SummaryResult = bigRClient.execute[SummaryResult](cmd4).result

		assertEquals(r4.ListResults("1")("mpg").min, 15.0, 0.01)
		assertEquals(r4.ListResults("1")("disp").min, 71.1, 0.01)
		assertEquals(r4.ListResults("1")("mpg").max, 33.9, 0.01)
		assertEquals(r4.ListResults("1")("disp").max, 351.0, 0.01)
		assertEquals(r4.ListResults("1")("mpg").first_quartile, 19.86, 0.01)
		assertEquals(r4.ListResults("1")("disp").first_quartile, 78.775, 0.01)
		assertEquals(r4.ListResults("1")("mpg").third_quartile, 28.46, 0.01)
		assertEquals(r4.ListResults("1")("disp").third_quartile, 150.62, 0.01)

		assertEquals(r4.ListResults("0")("mpg").min, 10.4, 0.01)
		assertEquals(r4.ListResults("0")("disp").min, 120.1, 0.01)
		assertEquals(r4.ListResults("0")("mpg").max, 24.4, 0.01)
		assertEquals(r4.ListResults("0")("disp").max, 472.0, 0.01)
		assertEquals(r4.ListResults("0")("mpg").first_quartile, 14.6, 0.01)
		assertEquals(r4.ListResults("0")("disp").first_quartile, 164.98, 0.01)
		assertEquals(r4.ListResults("0")("mpg").third_quartile, 19.2, 0.01)
		assertEquals(r4.ListResults("0")("disp").third_quartile, 356.25, 0.01)

		val cmd5 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear", "vs"), "mean")
		val r5: MeanResult = bigRClient.execute[MeanResult](cmd5).result
		assertEquals(r5.ListResults.size(), 6)
		assertEquals(r5.ListResults("5,0")("mpg"), 19.125, 0.01)
		assertEquals(r5.ListResults("5,0")("disp"), 229.325, 0.01)
		assertEquals(r5.ListResults("4,0")("mpg"), 21.0, 0.01)
		assertEquals(r5.ListResults("4,0")("disp"), 160.0, 0.01)
		assertEquals(r5.ListResults("3,1")("mpg"), 20.33, 0.01)
		assertEquals(r5.ListResults("3,1")("disp"), 201.033, 0.01)
		
		val cmd7 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear", "vs"), "summary")
		val r7: SummaryResult = bigRClient.execute[SummaryResult](cmd7).result
		assertEquals(r7.ListResults.size(), 6)
		assertEquals(r7.ListResults("5,0")("disp").min, 120.3, 0.01)
		assertEquals(r7.ListResults("5,0")("mpg").min, 15.0, 0.01)
		assertEquals(r7.ListResults("5,0")("disp").max,351.0, 0.01)
		assertEquals(r7.ListResults("5,0")("mpg").max, 26.0, 0.01)
		assertEquals(r7.ListResults("5,0")("mpg").first_quartile, 15.0, 0.01)
		assertEquals(r7.ListResults("5,0")("disp").first_quartile, 120.3, 0.01)
		assertEquals(r7.ListResults("5,0")("mpg").third_quartile, 19.7, 0.01)
		assertEquals(r7.ListResults("5,0")("disp").third_quartile, 301.0, 0.01)
		assertEquals(r7.ListResults("5,0")("mpg").median, 15.8, 0.01)
		assertEquals(r7.ListResults("5,0")("disp").median, 145.0, 0.01)
		
	}
}
