package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest

/**
 * author: daoduchuan
 */
class GenericExecutorSuite extends ABigRClientTest {

  test("test generic executor") {
    createTableMtcars
    val ddf = this.runSQL2RDDCmd("select * from mtcars", true)
    val dcID = ddf.dataContainerID
    LOG.info("Got dataContainerID= " + dcID)
    val cmd = new GenericExecutor("ddf.getMetaDataHandler.getNumRows", dcID)
    val result = bigRClient.execute[String](cmd)
    println(s">>> result = ${result.result}")
    assert(result.result == "32")

    val cmd2 = new GenericExecutor("ddf.getNumColumns", dcID)
    val result2 = bigRClient.execute[String](cmd2)
    println(s">>>>> numColumns = ${result2.result}")
    assert(result2.result == "11")
  }
}
