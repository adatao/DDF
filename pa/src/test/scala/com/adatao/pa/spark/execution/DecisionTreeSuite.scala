package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import io.ddf.ml.IModel

/**
 * author: daoduchuan
 */
class DecisionTreeSuite extends ABigRClientTest {

  test("test decision tree") {
    createTableMtcars
    val cmd1 = new Sql2DataFrame("select * from mtcars", true)
    val r1 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](cmd1).result

    val dataContainerId = r1.dataContainerID
    val command = new DecisionTree(dataContainerId, Array(0,1,2,3,4,5), 7, "Classification",
      maxDepth = 10)
    val model = bigRClient.execute[IModel](command).result
    assert(model != null)
  }
}
