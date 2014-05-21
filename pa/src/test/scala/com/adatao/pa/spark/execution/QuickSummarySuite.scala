package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.Subset.SubsetResult
import com.adatao.ddf.analytics.Summary
import org.junit.Assert.assertEquals

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 16/10/13
 * Time: 10:59 PM
 * To change this template use File | Settings | File Templates.
 */
class QuickSummarySuite extends ABigRClientTest{
	test("Test DataFrame"){
		createTableAirline
 		val df= this.runSQL2RDDCmd("select * from airline", true)
 		val dcID= df.dataContainerID
		
 		val cmd = new DDFExecutor("summary", dcID)
		val result= bigRClient.execute[Array[Summary]](cmd).result
		
		println(">>>>result=" + result)
		assertEquals(result(0).mean(), 2008, 0.1)

	}
	test("test Shark"){
		createTableMtcars
		val df= this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
		assert(df.isSuccess)
		val dcID= df.dataContainerID
		val cmd= new QuickSummary()
		cmd.setDataContainerID(dcID)
		val result= bigRClient.execute[DataframeStatsResult](cmd).result

		assert(result.mean(0) == 20.09)
		assert(result.mean(1) == 6.19)
		assert(result.min(2) == 71.1)
		assert(result.max(2) == 472.0)

		val jsCreateVectors=String.format("{columns: [{type: Column, name: hp}], dataContainerID: %s}", dcID)
		val cmd2= new Subset
		val result2= bigRClient.execute[SubsetResult]("Subset",jsCreateVectors)

		assert(result2.isSuccess)
		val subset= result2.result.dataContainerID

		val cmd3= new QuickSummary().setDataContainerID(subset)

		val result3= bigRClient.execute[DataframeStatsResult](cmd3).result
		assert(result3.mean.length == 1)
		assert(result3.mean(0) == 146.69)
		assert(result3.min.length == 1)
		assert(result3.max.length == 1)

	}
}
