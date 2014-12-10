package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import scala.collection.JavaConversions._
/**
 * author: daoduchuan
 */
class GraphSuite extends ABigRClientTest {

  test("test graphtfidf") {
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

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => Array(arr(0), arr(1), arr(2).toDouble)}
    result.map{
      row => println(row.mkString(", "))
    }
    assert(result(0)(0) == "SFO")
    assert(result(0)(1) == "SAN")
    assert(result(0)(2) == 1.2148111519023101)

    assert(result(1)(0) == "SFO")
    assert(result(1)(1) == "SJC")
    assert(result(1)(2) == 0.20827853703164503)

    assert(result(2)(0) == "SMF")
    assert(result(2)(1) == "LAX")
    assert(result(2)(2) == 1.0123426265852582)

    assert(result(3)(0) == "SMF")
    assert(result(3)(1) == "SJC")
    assert(result(3)(2) == 0.3471308950527417)

    assert(result(5)(0) == "SNA")
    assert(result(5)(1) == "OAK")
    assert(result(5)(2) == 0.8282803308424841)
  }

  test("test Cosine Similarity") {
    createTableGraph1
    val loader = new Sql2DataFrame("select * from graph1", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
    val dataContainerID = r0.dataContainerID
    val cmd = new GraphTFIDF(dataContainerID, "source", "dest")
    val r = bigRClient.execute[DataFrameResult](cmd)
    val fetchRows = new FetchRows().setDataContainerID(r.result.dataContainerID).setLimit(200)
    val r2 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r2.result.getData

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    result.map{
      row => println(row.mkString(","))
    }
  }
}
