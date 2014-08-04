package com.adatao.pa.spark.execution

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.adatao.spark.ddf.analytics.LinearRegressionModel
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import com.adatao.pa.spark.execution.GetURI.StringResult

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.types.ExecutionResult
import java.util.HashMap
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import java.lang.Integer
import com.adatao.pa.spark.execution.FiveNumSummary._
import com.adatao.pa.spark.execution.SetDDFName.SetDDFNameResult


class GetDDFSuite extends ABigRClientTest {
  test("Categorical variables linear regression normal equation on Shark") {
    createTableAirline

    val loader = new Sql2DataFrame("select * from airline", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)

    val dataContainerId =  r0.dataContainerID

    println(">>>>>>>>>>>>>>>>>>>>> dataContainerId = " + dataContainerId)

    //first set name
    val setddf = new SetDDFName(dataContainerId, "my_awsome_ddf")
    val r1 = bigRClient.execute[SetDDFNameResult](setddf).result
    println(">>>>>>>>>>> after setting get r1.dataContainerID= " + r1.uri)

    val getddf = new GetDDF("ddf://adatao.com/my_awsome_ddf")
    val r2 = bigRClient.execute[com.adatao.pa.spark.Utils.MutableDataFrameResult](getddf).result
    assert(r2.isSuccess)
    println(">>>>>>>>>>> after getting get r2.dataContainerID= " + r2.dataContainerID)
    assertEquals(r2.isMutable, false)

    //assertEquals(r1.uri, r2.dataContainerID)
    //get URI
    val uri = new GetURI().setDataContainerID(r0.dataContainerID)
    val resUri = bigRClient.execute[StringResult](uri)
    assert(resUri.isSuccess)
    println(">>>>>>>>>>> URI = " + resUri.result.str)
  }
}
