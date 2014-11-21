package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import scala.collection.JavaConversions._
/**
 * author: daoduchuan
 */
class GraphSuite extends ABigRClientTest {

  test("test graph") {
    createTableGraph
    val loader = new Sql2DataFrame("select * from graph", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
    val dataContainerID = r0.dataContainerID
    val cmd = new GraphTFIDF(dataContainerID, "source", "dest")
    val r = bigRClient.execute[DataFrameResult](cmd)
    val fetchRows = new FetchRows().setDataContainerID(r.result.dataContainerID).setLimit(10)
    val r2 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r2.result.getData

    ls.map{
      row => println(">>> row = " + row)
    }

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => Array(arr(0), arr(1), arr(2).toDouble)}
    //>>> row = "BUR"	"OAK"	1.7142857142857142
    //>>> row = "BUR"	"SFO"	0.2857142857142857
    //>>> row = "LAX"	"SFO"	1.7142857142857142

    assert(result(0)(0) == "BUR")
    assert(result(0)(1) == "OAK")
    assert(result(0)(2) == 1.7142857142857142)

    assert(result(0)(0) == "LAX")
    assert(result(0)(1) == "SFO")
    assert(result(0)(2) == 1.7142857142857142)

    assert(result(0)(0) == "BUR")
    assert(result(0)(1) == "SFO")
    assert(result(0)(2) == 0.2857142857142857)

    assert(r.isSuccess)
  }
}
