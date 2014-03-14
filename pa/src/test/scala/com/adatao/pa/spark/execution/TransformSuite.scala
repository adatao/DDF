package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult

/**
 * @author aht
 */
class TransformSuite extends ABigRClientTest {

	test("transform scale standard on file") {
		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		val scaler = new TransformScaleStandard(dataContainerId)
		val scaleres = bigRClient.execute[DataFrameResult](scaler)

		val exsum = new QuickSummary().setDataContainerID(scaleres.result.dataContainerID)
		val summary = bigRClient.execute[DataframeStatsResult](exsum)

		assert(summary.result.mean.forall(x => math.abs(x - 0) < 0.02))
		assert(summary.result.stdev.forall(x => math.abs(x - 1) < 0.02))
	}

	test("transform scale min-max on file") {
		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")
		val scaler = new TransformScaleMinMax(dataContainerId)
		val scaleres = bigRClient.execute[DataFrameResult](scaler)

		val exsum = new QuickSummary().setDataContainerID(scaleres.result.dataContainerID)
		val summary = bigRClient.execute[DataframeStatsResult](exsum)

		assert(summary.result.min.forall(x => math.abs(x - 0) < 0.02))
		assert(summary.result.max.forall(x => math.abs(x - 1) < 0.02))
	}

	test("transform scale on shark") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		// scale standard
		val dataContainerId = r0.result.dataContainerID
		val scaler = new TransformScaleStandard(dataContainerId)
		val scaleres = bigRClient.execute[DataFrameResult](scaler)

		val exsum = new QuickSummary().setDataContainerID(scaleres.result.dataContainerID)
		val summary = bigRClient.execute[DataframeStatsResult](exsum)

		assert(summary.result.mean.forall(x => math.abs(x - 0) < 0.02))
		assert(summary.result.stdev.forall(x => math.abs(x - 1) < 0.02))

		// scale min max
		val scaler2 = new TransformScaleMinMax(dataContainerId)
		val scaleres2 = bigRClient.execute[DataFrameResult](scaler2)

		val exsum2 = new QuickSummary().setDataContainerID(scaleres2.result.dataContainerID)
		val summary2 = bigRClient.execute[DataframeStatsResult](exsum2)

		assert(summary2.result.min.forall(x => math.abs(x - 0) < 0.02))
		assert(summary2.result.max.forall(x => math.abs(x - 1) < 0.02))
	}

	test("transform scale preserving factors") {
		createTableMtcars

		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader)
		assert(r0.isSuccess)

		bigRClient.execute[GetFactor.GetFactorResult](new GetFactor().setDataContainerID(r0.result.dataContainerID).setColumnIndex(1))  // "cyl" column

		val dataContainerId = r0.result.dataContainerID
		val scaler = new TransformScaleStandard(dataContainerId)
		val scaleres = bigRClient.execute[DataFrameResult](scaler)

		val exsum = new QuickSummary().setDataContainerID(scaleres.result.dataContainerID)
		val summary = bigRClient.execute[DataframeStatsResult](exsum)

		assert(math.abs(summary.result.mean(1) - 6.1) < 0.1)
	}
}
