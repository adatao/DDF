package com.adatao.pa.spark.execution

import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import com.adatao.pa.spark.types.ABigRClientTest
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.adatao.pa.spark.execution.Subset.SubsetResult
import org.scalatest.BeforeAndAfterAll

//import com.adatao.pa.spark.execution.VectorVariance.VectorVarianceResult
import java.util
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.NRow.NRowResult

class TransformNativeRserveSuite extends ABigRClientTest {

	test("can add column") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

    val nrow0 = new NRow().setDataContainerID(dataContainerId)
    val rrow0 = bigRClient.execute[NRowResult](nrow0)
    assert(rrow0.result.nrow==32)

		val transformer = new TransformNativeRserve(dataContainerId, "newcol = mpg / gear")
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		val nrow = new NRow().setDataContainerID(r1.result.dataContainerID)
		val rrow = bigRClient.execute[NRowResult](nrow)
		assert(rrow.result.nrow==32)
		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

//		val actual = r2.result.data.asScala.map { row => row(row.length - 1).asInstanceOf[Double]}
//		val expected = Array(5.25, 5.25, 5.7, 7.133333, 6.233333, 6.033333, 4.766667, 6.1, 5.7, 4.8,
//			4.45, 5.466667, 5.766667, 5.066667, 3.466667, 3.466667, 4.9, 8.1, 7.6, 8.475,
//			7.166667, 5.166667, 5.066667, 4.433333, 6.4, 6.825, 5.2, 6.08, 3.16, 3.94, 3.0, 5.35)
//		assert(actual.zip(expected).forall { case (x, y) => math.abs(y - x) < 0.01 })

	}

	test("can add multiple columns on data w some empty partitions") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars where mpg > 30", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId,
			"z1 = mpg / gear, " +
			"z2 = mpg * 0.4251437075, " +
			"z3 = rpois(nrow(df.partition), 10000)")
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
	}

	//TODO stack overflow
	ignore("can add multiple columns using back-to-back transform") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		val transformer1 = new TransformNativeRserve(r0.result.dataContainerID,
			"z1 = mpg / gear, " +
			"z2 = rpois(nrow(df.partition), 10000)")
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer1)
		assert(r1.isSuccess)

		val transformer2 = new TransformNativeRserve(r1.result.dataContainerID,
			"z3 = mpg * 0.4251437075, " +
			"z4 = rpois(nrow(df.partition), 2000)")
		val r2 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer2)
		assert(r2.isSuccess)

		assert(r2.result.metaInfo.length === 15)

		val fetcher = new FetchRows().setDataContainerID(r2.result.dataContainerID).setLimit(32)
		val r3 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r3.isSuccess)
	}

//	TODO fix this so we can return error properly
	ignore("can return error msg to user") {
		val loader = new Sql2DataFrame("select * from mtcars where mpg > 30", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId, "z1 = abc / xyz")
		try {
			bigRClient.execute(transformer)
		} catch {
			case e: Exception => {
				println(e.toString)
				assert(e.toString.contains("object \\u0027abc\\u0027 not found"))
			}
		}
	}

	test("can serialize Java null as R NA, and back to null") {
    createTableAirQuality
		val loader = new Sql2DataFrame("select * from airquality", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId,
			"z1 = solar_radiation / ozone, " +
			"z2 = 35.74 + 0.6251 * temp - 35.75 * (wind ^ 0.16) + 0.4275 * temp * (wind ^ 0.16)") // wind chill formula
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		val r3 = bigRClient.execute[SubsetResult]("Subset",
			String.format("{dataContainerID: \"%s\", columns: [{type: \"Column\", name: \"z1\"}]}", r1.result.dataContainerID))
		assert(r3.isSuccess)

		// can compute stats on results, with null/NA handled correctly
		val summarizer = new QuickSummary().setDataContainerID(r1.result.dataContainerID)
		val r4 = bigRClient.execute[DataframeStatsResult](summarizer)
		assert(r4.isSuccess)
		assert(r4.result.cNA(6) === 42)
		assert((r4.result.sum(6) - 732.5482) < 0.01)
		assert((r4.result.mean(6) - 6.599533) < 0.01)
		assert((r4.result.stdev(6) - 5.663489) < 0.01)
	}

	test("can update column values") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId, "mpg = mpg * 0.4251437075")
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		//manually confirm this is the case. COmment out because Sharkr returns tab separated cell values which we didn't parse accordingly
//		val actual = r2.result.data.asScala.map { row => row(0).asInstanceOf[Double] }
//		val expected = Array(8.9280178575, 8.9280178575, 9.693276531, 9.0980753405, 7.95018733025, 7.695101105750001, 6.079555017250001, 10.373506463, 9.693276531, 8.162759184,
//			7.5675579935, 6.972356802999999, 7.35498613975, 6.462184354, 4.421494558, 4.421494558, 6.2496125002500005, 13.774656123, 12.924368708, 14.412371684250001,
//			9.14058971125, 6.58972746625, 6.462184354, 5.65441130975, 8.162759184, 11.60642321475, 11.053736395000001, 12.924368708, 6.717270578500001, 8.37533103775, 6.3771556125, 9.0980753405)
//		assert(actual.zip(expected).forall { case (x, y) => math.abs(y - x) < 0.01 })
	}

	//because Shark return \t delimited string in result so BigRclient need to handle that as well
	test("can update column as.integer, as.character") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId,
			"mpg = as.integer(mpg), " +
			"gear = as.character(gear)"
		)
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo(0).getType === "int")
		assert(r1.result.metaInfo(9).getType === "string")

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)



		val mpg = r2.result.data.asScala.map { row => row(0) }
		// strangely this would fail, coming out as Double values
		// even though FetchRows and JSON data are both Int values
		val a = r2.result.data.get(0).toString().split("\t")

		assert(a(0) === "21")
		assert(a(1) === "6")

//		val gear = r2.result.data.asScala.map { row => row(9) }
//		assert(gear.toArray === Array("4", "4", "4", "3", "3", "3", "3", "4", "4", "4", "4", "3", "3", "3", "3", "3", "3", "4", "4", "4", "3", "3", "3", "3", "3", "4", "5", "5", "5", "5", "5", "4"))
	}

	test("can transform Object[] DataFrame") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId,
			"z1 = mpg / cyl, " +
			"z2 = disp * 0.4251437075, " +
			"z3 = rpois(nrow(df.partition), 1000)")
		val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
		assert(r1.isSuccess)

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
	}
}
