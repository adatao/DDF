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
    
    val getddf = new GetDDF(dataContainerId)
    val r1 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](getddf).result
    assert(r1.isSuccess)
    
    
    println(">>>>>>>>>>>>>>>>> returned ddf with datacontainerId = " + r1.dataContainerID)
    

  }
}