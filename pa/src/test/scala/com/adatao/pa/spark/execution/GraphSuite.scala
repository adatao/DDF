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
    val fetchRows = new FetchRows().setDataContainerID(r.result.dataContainerID).setLimit(100)
    val r2 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r2.result.getData
    ls.map{
      row => println(">>> row = " + row)
    }
    assert(r.isSuccess)
  }
}
