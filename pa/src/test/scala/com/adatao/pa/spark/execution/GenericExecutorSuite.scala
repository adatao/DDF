package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest

/**
 * author: daoduchuan
 */
class GenericExecutorSuite extends ABigRClientTest {


//  test("test generic executor without argument") {
//    createTableMtcars
//    val ddf = this.runSQL2RDDCmd("select * from mtcars", true)
//    val dcID = ddf.dataContainerID
//    LOG.info("Got dataContainerID= " + dcID)
//    val cmd = new GenericExecutor( dcID, "ddf.getMetaDataHandler.getNumRows")
//    val result = bigRClient.execute[String](cmd)
//    println(s">>> result = ${result.result}")
//    assert(result.result == "32")
//
//    val cmd2 = new GenericExecutor(dcID, "ddf.getNumColumns")
//    val result2 = bigRClient.execute[String](cmd2)
//    println(s">>>>> numColumns = ${result2.result}")
//    assert(result2.result == "11")
//  }

  test("test generic executor with params") {
    createTableAirline
    val ddf = this.runSQL2RDDCmd("select * from airline", true)
    val dcID = ddf.dataContainerID

    val cmd = new GenericExecutor(dcID, "ddf.getVectorQuantiles",  Array("v1", "[0.1, 0.5, 0.9]"))

    val result = bigRClient.execute[String](cmd)
    println(s">>>>> quantile result = ${result.result}")

    val cmd2 = new GenericExecutor(dcID, "ddf.Views.getRandomSample", Array("10", "false", "0"))
    val result2 = bigRClient.execute[String](cmd2)
    assert(result2.result != null)
    print(s"ddf.Views.getRandomSample = ${result2.result}")
  }
//
//  test("test generic executor for kmeans") {
//    createTableAirline
//    val ddf = this.runSQL2RDDCmd("select v1, v2, v3 from airline", true)
//    val dcID = ddf.dataContainerID
//
//    val cmd = new GenericExecutor(dcID, "ddf.ML.kMeans", Array("10", "10", "10", "random"))
//    val result = bigRClient.execute[String](cmd)
//    println(s">>>>> imodel = ${result.result}")
//  }
}
