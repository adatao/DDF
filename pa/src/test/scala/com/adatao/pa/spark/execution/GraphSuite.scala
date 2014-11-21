package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult

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
    val cmd = new CreateGraph(dataContainerID, 0, 1)
    val r = bigRClient.execute[DataFrameResult](cmd)
    assert(r.isSuccess)
  }
}
