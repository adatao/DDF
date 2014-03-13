package adatao.bigr.spark.execution

import org.junit.Assert._
import adatao.bigr.spark.types.ABigRClientTest
import adatao.bigr.spark.execution.Subset.{SubsetResult, Column}
import adatao.bigr.spark.execution.VectorMean._
import adatao.bigr.spark.execution.VectorVariance._
import adatao.bigr.spark.execution.VectorHistogram._
import scala.collection.JavaConverters._
import adatao.bigr.spark.execution.FetchRows._
import adatao.bigr.spark.execution.VectorCovariance._
import adatao.bigr.spark.execution.VectorCorrelation._

class VectorStatsSuite extends ABigRClientTest {
	test("Test CSV") {
		val dfID = this.loadFile("resources/mtcars", false, " ");
		var col = new Column()
		col.setIndex(2)
		col.setType("Column")
		var executor = new Subset()
		executor.setDataContainerID(dfID)
		executor.setColumns(List(col).asJava)

		var res: SubsetResult = bigRClient.execute[SubsetResult](executor).result
		var dcID = res.getDataContainerID

		// Get Mean
		val executor2 = new VectorMean()
		executor2.setDataContainerID(dcID)
		val res2: VectorMeanResult = bigRClient.execute[VectorMeanResult](executor2).result
		LOG.info("Mean Result = " + res2.getMean())
		assertEquals(res2.getMean(), 230.72, 0.01)

		// Get Variance
		val executor3 = new VectorVariance()
		executor3.setDataContainerID(dcID)
		val res3: VectorVarianceResult = bigRClient.execute[VectorVarianceResult](executor3).result
		LOG.info("Variance Result = " + res3.getVariance())
		assertEquals(res3.getVariance(), 15360.8, 0.01)
		assertEquals(res3.getStdDev(), 123.9387, 0.0001)
		
		// Histogram
		col = new Column()
		col.setIndex(0)
		col.setType("Column")
		executor = new Subset()
		executor.setDataContainerID(dfID)
		executor.setColumns(List(col).asJava)

		res = bigRClient.execute[SubsetResult](executor).result
		dcID = res.getDataContainerID
		
		val executor4 = new VectorHistogram()
		executor4.setDataContainerID(dcID).setNumBins(5)
		
		val res4: VectorHistogramResult = bigRClient.execute[VectorHistogramResult](executor4).result
		assertEquals(true, res4.success)
		assertEquals(5, res4.histogramBins.size())
	}
	
	test("Test filter NAs") {
		val dfID = this.loadFile("resources/airline.csv", false, ",");
		var col = new Column()
		col.setIndex(28)
		col.setType("Column")
		var executor = new Subset()
		executor.setDataContainerID(dfID)
		executor.setColumns(List(col).asJava)

		var res: SubsetResult = bigRClient.execute[SubsetResult](executor).result
		var dcID = res.getDataContainerID

		// Get Mean
		val executor2 = new VectorMean()
		executor2.setDataContainerID(dcID)
		val res2: VectorMeanResult = bigRClient.execute[VectorMeanResult](executor2).result
		LOG.info("Mean Result = " + res2.getMean())
		assertEquals(res2.getMean(), 31.33, 0.01)

		// Get Variance
		val executor3 = new VectorVariance()
		executor3.setDataContainerID(dcID)
		val res3: VectorVarianceResult = bigRClient.execute[VectorVarianceResult](executor3).result
		LOG.info("Variance Result = " + res3.getVariance())
		assertEquals(res3.getVariance(), 535, 0.01)
		assertEquals(res3.getStdDev(), 23.13007, 0.0001)
		
		// Histogram
		val executor4 = new VectorHistogram()
		executor4.setDataContainerID(dcID).setNumBins(5)
		
		val res4: VectorHistogramResult = bigRClient.execute[VectorHistogramResult](executor4).result
		assertEquals(true, res4.success)
		assertEquals(5, res4.histogramBins.size())
	}
	
	test("Test Shark") {
		createTableMtcars
		
		val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
		assert(df.isSuccess)
		var col = new Column()
		col.setName("mpg")
		col.setType("Column")
		var executor = new Subset()
		executor.setDataContainerID(df.dataContainerID)
		executor.setColumns(List(col).asJava)

		var res: SubsetResult = bigRClient.execute[SubsetResult](executor).result
		var dcID = res.getDataContainerID

		// Get Mean
		val executor2 = new VectorMean()
		executor2.setDataContainerID(dcID)
		val res2: VectorMeanResult = bigRClient.execute[VectorMeanResult](executor2).result
		LOG.info("Mean Result = " + res2.getMean())
		assertEquals(20.09062, res2.getMean(), 0.00001)

		// Get Variance
		val executor3 = new VectorVariance()
		executor3.setDataContainerID(dcID)
		val res3: VectorVarianceResult = bigRClient.execute[VectorVarianceResult](executor3).result
		LOG.info("Variance Result = " + res3.getVariance())
		assertEquals(36.3241, res3.getVariance(), 0.0001)
		assertEquals(6.026948, res3.getStdDev(), 0.00001)
		
		// Histogram
		val executor4 = new VectorHistogram()
		executor4.setDataContainerID(dcID).setNumBins(5)
		
		val res4: VectorHistogramResult = bigRClient.execute[VectorHistogramResult](executor4).result
		assertEquals(true, res4.success)
		assertEquals(5, res4.histogramBins.size())
		
		// extract hp column
		col = new Column()
		col.setName("hp")
		col.setType("Column")
		executor = new Subset()
		executor.setDataContainerID(df.dataContainerID)
		executor.setColumns(List(col).asJava)

		res = bigRClient.execute[SubsetResult](executor).result
		val hpID = res.getDataContainerID
		
		val executor5 = new VectorCovariance()
		executor5.setxDataContainerID(dcID).setyDataContainerID(hpID)
		
		val res5: VectorCovarianceResult = bigRClient.execute[VectorCovarianceResult](executor5).result
		assertEquals(true, res5.success)
		assertEquals(-320.7321, res5.getCovariance, 0.0001)
		
		val executor6 = new VectorCorrelation()
		executor6.setxDataContainerID(dcID).setyDataContainerID(hpID)
		
		val res6: VectorCorrelationResult = bigRClient.execute[VectorCorrelationResult](executor6).result
		assertEquals(true, res6.success)
		assertEquals(-0.7761684, res6.getCorrelation, 0.1)
		
	}
}