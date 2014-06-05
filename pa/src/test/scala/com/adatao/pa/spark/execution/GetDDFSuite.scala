package com.adatao.pa.spark.execution

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.ML.LinearRegressionModel
import com.adatao.ML.LogisticRegressionModel
//import com.adatao.spark.ddf.analytics.LogisticRegressionModel

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.types.ExecutionResult
import java.util.HashMap
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import java.lang.Integer
import com.adatao.pa.spark.execution.FiveNumSummary._

class GetDDFSuite extends ABigRClientTest {
  test("Categorical variables linear regression normal equation on Shark") {
    createTableAirline

    val loader = new Sql2DataFrame("select * from airline", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId = "SparkDDF_spark_" + r0.dataContainerID.replace("-", "_");

    println(">>>>>>>>>>>>>>>>>>>>> dataContainerId = " + dataContainerId)

    //first set name
    var ddfName = "my_awsome_ddf"
    val setddf = new SetDDFName(dataContainerId, ddfName)
    val r1 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](setddf).result
    println(">>>>>>>>>>> after setting get r1.dataContainerID= " + r1.dataContainerID)

    ddfName= "ddf://adatao.com/my_awsome_ddf"
    val getddf = new GetDDF(ddfName)
    val r2 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](getddf).result
    assert(r2.isSuccess)
    println(">>>>>>>>>>> after getting get r2.dataContainerID= " + r2.dataContainerID)

    assertEquals(r1.dataContainerID, r2.dataContainerID)

  }
}