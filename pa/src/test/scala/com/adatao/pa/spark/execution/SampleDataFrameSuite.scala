package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.SampleDataFrame._
import com.adatao.pa.spark.execution.FetchRows._
import scala.collection.JavaConversions._

class SampleDataFrameSuite extends ABigRClientTest {
	test("test SampleDataFrame") {

		//val dcID = this.loadFile("resources/mtcars", false, " ") //   Required DDF backed by table

		createTableMtcars
    val loader = new Sql2DataFrame("select * from mtcars", true)
		  val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dcID = r0.dataContainerID

		val cmd1 = new SampleDataFrame().setDataContainerID(dcID).setSize(2).setReplace(false).setGetPercent(false)
		val res1 = bigRClient.execute[SampleDataFrameSizeResult](cmd1)
		assert(res1.isSuccess == true)

		assert(res1.result.data.get(0)(0).asInstanceOf[Double] === 33.9)
		assert(res1.result.data.get(0)(1).asInstanceOf[Double] === 4.0)
		assert(res1.result.data.get(0)(3).asInstanceOf[Double] === 65.0)

		val cmd2 = new SampleDataFrame().setDataContainerID(dcID).setPercent(0.5).setReplace(false).setGetPercent(true)
		val res2 = bigRClient.execute[SampleDataFramePercentResult](cmd2)
		assert(res2.isSuccess == true)

//		val cmd3 = new FetchRows().setDataContainerID(res2.result.dataContainerID).setLimit(10)
//		val res3 = bigRClient.execute[FetchRowsResult](cmd3)
//		assert(res3.isSuccess == true)
//
//		assert(res3.result.data.get(0)(0).asInstanceOf[Double] === 21.0)
//		assert(res3.result.data.get(0)(1).asInstanceOf[Double] === 6)
//		assert(res3.result.data.get(0)(2).asInstanceOf[Double] === 160)
//		assert(res3.result.data.get(0)(5).asInstanceOf[Double] === 2.620)
	}
	
	test("test SampleDataFrame on SharkDataFrame") {
		createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dcID = r0.dataContainerID
		val cmd1 = new SampleDataFrame().setDataContainerID(dcID).setSize(2).setReplace(false).setGetPercent(false)
		val res1 = bigRClient.execute[SampleDataFrameSizeResult](cmd1)
		assert(res1.isSuccess == true)

		assert(math.abs(res1.result.data.get(0)(0).asInstanceOf[Double] - 33.9) < 0.01)

		val cmd2 = new SampleDataFrame().setDataContainerID(dcID).setPercent(0.5).setReplace(false).setGetPercent(true)
		val res2 = bigRClient.execute[SampleDataFramePercentResult](cmd2)
		assert(res2.isSuccess == true)
//		Required DDF backed by table
//		val cmd3 = new FetchRows().setDataContainerID(res2.result.dataContainerID).setLimit(10)
//		val res3 = bigRClient.execute[FetchRowsResult](cmd3)
//		assert(res3.isSuccess == true)
//		assert(math.abs(res3.result.data.get(0)(0).asInstanceOf[Double] - 21) < 0.01)
//
	}
}
