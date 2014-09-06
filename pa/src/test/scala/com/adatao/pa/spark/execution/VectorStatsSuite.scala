package com.adatao.pa.spark.execution

import org.junit.Assert._
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.Subset.SubsetResult
//import io.ddf.content.ViewHandler.Column
import com.adatao.pa.spark.execution.VectorMean._
import com.adatao.pa.spark.execution.VectorVariance._
import scala.collection.JavaConverters._
import com.adatao.pa.spark.execution.FetchRows._
import com.adatao.pa.spark.execution.VectorCorrelation._

class VectorStatsSuite extends ABigRClientTest {
	test("Test CSV") {
    createTableAirline
    //		val dfID = this.loadFile("resources/mtcars", false, " ");
    val loader = new Sql2DataFrame("select * from airline", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
    val dfID = r0.dataContainerID

    // Get Mean
    val executor2 = new VectorMean()
    executor2.setDataContainerID(dfID)
    executor2.setColumnName("v1")
    val res2: VectorMeanResult = bigRClient.execute[VectorMeanResult](executor2).result
    assert(res2.isSuccess)

    System.out.println(">>>> Variance Mean = " + res2.getMean())
    LOG.info("Mean Result = " + res2.getMean())
    assertEquals(res2.getMean(), 2008.096, 0.01)

    // Get Variance
    val executor3 = new VectorVariance()
    executor3.setDataContainerID(dfID)
    executor3.setColumnName("v1")

    val res3: VectorVarianceResult = bigRClient.execute[VectorVarianceResult](executor3).result
    LOG.info("Variance Result = " + res3.getVariance())
    System.out.println(">>>> Variance Result = " + res3.getVariance())
    assertEquals(res3.getVariance(), 0.156, 0.01)
    assertEquals(res3.getStdDev(), 0.394, 0.01)
  }
}
