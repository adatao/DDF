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
import com.adatao.pa.spark.execution.Aggregate.AggregateResult

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
		val r1: AggregateResult = bigRClient.execute[AggregateResult](cmd1).result

		assertEquals(r1.results("5")(0),  21.38, 0.01)
		assertEquals(r1.results("5")(1), 202.48, 0.01)
		assertEquals(r1.results("4")(0), 24.53, 0.01)
		assertEquals(r1.results("4")(1), 123.01, 0.01)
		assertEquals(r1.results("3")(0), 16.10, 0.01)
		assertEquals(r1.results("3")(1), 326.3, 0.01)

		val cmd2 = new Aggregate(dcID, Array("wt", "qsec"), Array("am"), "median")
		val r2: AggregateResult = bigRClient.execute[AggregateResult](cmd2).result
		assertEquals(r2.results("1")(0), 2.26, 0.01)
		assertEquals(r2.results("1")(1), 16.96, 0.01)
		assertEquals(r2.results("0")(0), 3.49, 0.01)
		assertEquals(r2.results("0")(1), 17.71, 0.01)

		val cmd3 = new Aggregate(dcID, Array("disp", "hp", "drat"), Array("carb"), "variance")
		val r3: AggregateResult = bigRClient.execute[AggregateResult](cmd3).result

		assertEquals(r3.results("6")(0), 0.0, 0.01)
		assertEquals(r3.results("1")(0), 4937.884, 0.01)
		assertEquals(r3.results("2")(1), 1739.56, 0.01)
		assertEquals(r3.results("1")(2), 0.263, 0.01)

		val cmd6 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear"), "sum")
		val r6: AggregateResult = bigRClient.execute[AggregateResult](cmd6).result

		assertEquals(r6.results("5")(0), 106.9, 0.01)
		assertEquals(r6.results("5")(1), 1012.4, 0.01)
		assertEquals(r6.results("3")(1), 4894.5, 0.01)

//		val cmd4 = new Aggregate(dcID, Array("mpg", "disp"), Array("am"), "summary")
//		val r4: AggregateResult = bigRClient.execute[AggregateResult](cmd4).result
//
//		assertEquals(r4.results("1")("mpg").min, 15.0, 0.01)
//		assertEquals(r4.results("1")("disp").min, 71.1, 0.01)
//		assertEquals(r4.results("1")("mpg").max, 33.9, 0.01)
//		assertEquals(r4.results("1")("disp").max, 351.0, 0.01)
//		assertEquals(r4.results("1")("mpg").first_quartile, 19.86, 0.01)
//		assertEquals(r4.results("1")("disp").first_quartile, 78.775, 0.01)
//		assertEquals(r4.results("1")("mpg").third_quartile, 28.46, 0.01)
//		assertEquals(r4.results("1")("disp").third_quartile, 150.62, 0.01)
//
//		assertEquals(r4.results("0")("mpg").min, 10.4, 0.01)
//		assertEquals(r4.results("0")("disp").min, 120.1, 0.01)
//		assertEquals(r4.results("0")("mpg").max, 24.4, 0.01)
//		assertEquals(r4.results("0")("disp").max, 472.0, 0.01)
//		assertEquals(r4.results("0")("mpg").first_quartile, 14.6, 0.01)
//		assertEquals(r4.results("0")("disp").first_quartile, 164.98, 0.01)
//		assertEquals(r4.results("0")("mpg").third_quartile, 19.2, 0.01)
//		assertEquals(r4.results("0")("disp").third_quartile, 356.25, 0.01)

		val cmd5 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear", "vs"), "mean")
		val r5: AggregateResult = bigRClient.execute[AggregateResult](cmd5).result
		assertEquals(r5.results.size(), 6)
		assertEquals(r5.results("5,0")(0), 19.125, 0.01)
		assertEquals(r5.results("5,0")(1), 229.325, 0.01)
		assertEquals(r5.results("4,0")(0), 21.0, 0.01)
		assertEquals(r5.results("4,0")(1), 160.0, 0.01)
		assertEquals(r5.results("3,1")(0), 20.33, 0.01)
		assertEquals(r5.results("3,1")(1), 201.033, 0.01)
		
//		val cmd7 = new Aggregate(dcID, Array("mpg", "disp"), Array("gear", "vs"), "summary")
//		val r7: SummaryResult = bigRClient.execute[SummaryResult](cmd7).result
//		assertEquals(r7.results.size(), 6)
//		assertEquals(r7.results("5,0")("disp").min, 120.3, 0.01)
//		assertEquals(r7.results("5,0")("mpg").min, 15.0, 0.01)
//		assertEquals(r7.results("5,0")("disp").max,351.0, 0.01)
//		assertEquals(r7.results("5,0")("mpg").max, 26.0, 0.01)
//		assertEquals(r7.results("5,0")("mpg").first_quartile, 15.0, 0.01)
//		assertEquals(r7.results("5,0")("disp").first_quartile, 120.3, 0.01)
//		assertEquals(r7.results("5,0")("mpg").third_quartile, 19.7, 0.01)
//		assertEquals(r7.results("5,0")("disp").third_quartile, 301.0, 0.01)
//		assertEquals(r7.results("5,0")("mpg").median, 15.8, 0.01)
//		assertEquals(r7.results("5,0")("disp").median, 145.0, 0.01)
		
	}
}
