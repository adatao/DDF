package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import com.adatao.pa.spark.execution.Subset.SubsetResult
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult

class TransformHiveSuite extends ABigRClientTest {

  test("Can add column") {
    createTableAirline
    createTableMtcars
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID

    val cmd = new TransformHive(dcID, "is_sfo = if(v9=\"SFO\",1,0), foobar = v1 - v2, speed = v3 / v4")
    val result = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](cmd)
    assert(result.isSuccess)
  }

  test("can add multiple columns using back-to-back transform") {
    val loader = new Sql2DataFrame("select * from mtcars", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
    assert(r0.isSuccess)

    val transformer1 = new TransformHive(r0.result.dataContainerID,
      "z1 ~ mpg / gear")
    val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer1)
    assert(r1.isSuccess)

    val transformer2 = new TransformHive(r1.result.dataContainerID,
      "z3 ~ mpg * 0.4251437075")
    val r2 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer2)
    assert(r2.isSuccess)

    assert(r2.result.metaInfo.length === 13)

    val fetcher = new FetchRows().setDataContainerID(r2.result.dataContainerID).setLimit(32)
    val r3 = bigRClient.execute[FetchRowsResult](fetcher)
    assert(r3.isSuccess)
  }

  test("can add multiple columns on data w some empty partitions") {
    val loader = new Sql2DataFrame("select * from mtcars where mpg > 30", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

    val transformer = new TransformHive(dataContainerId,
      "z1 ~ mpg / gear, " +
        "mpg * 0.4251437075")
    val r1 = bigRClient.execute[com.adatao.pa.spark.Utils.DataFrameResult](transformer)
    assert(r1.isSuccess)

    val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(32)
    val r2 = bigRClient.execute[FetchRowsResult](fetcher)
    assert(r2.isSuccess)
  }

  test("test transform on airquality table") {
    createTableAirQuality
    val loader = new Sql2DataFrame("select * from airquality", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = r0.dataContainerID

    val transformer = new TransformHive(dataContainerId,
      "z1 = solar_radiation / ozone, " +
        "z2 = 35.74 + 0.6251 * temp - 35.75 * (power(wind, 0.16)) + 0.4275 * temp * (power(wind, 0.16))") // wind chill formula
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
}