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

import org.junit.Assert._

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.Subset.{SubsetResult, Column}
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/7/13
 * Time: 2:42 PM
 * To change this template use File | Settings | File Templates.
 */
class VectorQuantilesSuite extends ABigRClientTest {
	test("test VectorQuantiles for SharkColumnVector") {
		this.runSQLCmd("set shark.test.data.path=resources")
		this.runSQLCmd("drop table if exists mtcars")
		this.runSQLCmd("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, " +
			"                   drat double, wt double, qsec double, vs int, am int, gear int, carb" +
			" int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
		this.runSQLCmd("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/mtcars' " +
			"INTO TABLE mtcars")
		val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
		assert(df.isSuccess)
		val col = new Column()
		col.setName("mpg")
		col.setType("Column")
		val executor = new Subset()
		executor.setDataContainerID(df.dataContainerID)
		executor.setColumns(List(col).asJava)

		val res: SubsetResult = bigRClient.execute[SubsetResult](executor).result

		val dcID = res.getDataContainerID
		val executor2 = new VectorQuantiles(dataContainerID = dcID, pArray = Array(0, 0.25, 0.5, 0.75, 1), B = 1000000)

		val result: Array[Double] = bigRClient.execute[Array[Double]](executor2).result
		assertEquals(result(0), 10.4, 0.01)
		assertEquals(result(1), 15.2, 0.01)
		assertEquals(result(2), 19.2, 0.01)
		assertEquals(result(3), 22.15, 0.01)
		assertEquals(result(4), 33.9, 0.01)

		val executor3 = new VectorQuantiles(dataContainerID = dcID, pArray = Array(0.0, 1.0), B = 1000000)
		val result3: Array[Double] = bigRClient.execute[Array[Double]](executor3).result
		assertEquals(result3(0), 10.4, 0.01)
		assertEquals(result3(1), 33.9, 0.01)
		
		val col2 = new Column()
		col2.setName("vs")
		col2.setType("Column")
		val executor4 = new Subset()
		executor4.setDataContainerID(df.dataContainerID)
		executor4.setColumns(List(col2).asJava)

		val res2: SubsetResult = bigRClient.execute[SubsetResult](executor4).result

		val dcID2 = res2.getDataContainerID
		val executor5 = new VectorQuantiles(dataContainerID = dcID2, pArray = Array(0, 0.25, 0.5, 0.75, 1, 0, 1), B = 1000000)

		val result2: Array[Double] = bigRClient.execute[Array[Double]](executor5).result
		assertEquals(result2(0), 0, 0.01)
		assertEquals(result2(1), 0, 0.01)
		assertEquals(result2(2), 0, 0.01)
		assertEquals(result2(3), 1, 0.01)
		assertEquals(result2(4), 1, 0.01)
		assertEquals(result2(5), 0, 0.01)
		assertEquals(result2(6), 1, 0.01)

		val executor6 = new VectorQuantiles(dataContainerID = dcID2, pArray = Array(0.5, 0.9, 0.99), B = 1000000)

		val result6: Array[Double] = bigRClient.execute[Array[Double]](executor6).result
	}
	
	test("test VectorQuantiles for normal Vector") {
		val dfID = this.loadFile("resources/mtcars", false, " ");
		val col = new Column()
		col.setName("V1")
		col.setType("Column")
		val executor = new Subset()
		executor.setDataContainerID(dfID)
		executor.setColumns(List(col).asJava)

		val res: SubsetResult = bigRClient.execute[SubsetResult](executor).result
		val dcID = res.getDataContainerID

		val executor2 = new VectorQuantiles(dataContainerID = dcID, pArray = Array(0, 0.25, 0.5, 0.75, 1), B = 1000000)

		val result: Array[Double] = bigRClient.execute[Array[Double]](executor2).result
		assertEquals(result(0), 10.4, 0.01)
		assertEquals(result(1), 15.2, 0.01)
		assertEquals(result(2), 19.2, 0.01)
		assertEquals(result(3), 22.15, 0.01)
		assertEquals(result(4), 33.9, 0.01)

		val executor3 = new VectorQuantiles(dataContainerID = dcID, pArray = Array(0.0, 1.0), B = 1000000)
		val result3: Array[Double] = bigRClient.execute[Array[Double]](executor3).result
		assertEquals(result3(0), 10.4, 0.01)
		assertEquals(result3(1), 33.9, 0.01)

	}
}
