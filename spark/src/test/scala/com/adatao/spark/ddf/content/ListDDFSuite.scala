package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.ATestSuite

/**
 * author: daoduchuan
 */
class ListDDFSuite extends ATestSuite {
  createTableMtcars()
  createTableAirline()

  test("test list ddf") {
    val ddf1 = manager.sql2ddf("select * from mtcars")
    val ddf2 = manager.sql2ddf("select * from airline")
    ddf1.setAliasName("mtcars")
    ddf2.setAliasName("airline")

    manager.addDDF(ddf1)
    manager.addDDF(ddf2)

    val listDDF = manager.listDDFs()

    listDDF.foreach{
      ddfinfo => LOG.info(s">>> numRows = ${ddfinfo.getNumRows}; numColumns = ${ddfinfo.getNumColumns}\n " +
        s"uri = ${ddfinfo.getUri}; createdTime = ${ddfinfo.getCreatedTime}")
    }

    assert(listDDF.size == 2)
    assert(listDDF(0).getNumRows != null)
    assert(listDDF(0).getNumColumns != null)
    assert(listDDF(0).getUri != null)
  }
}
