package com.adatao.spark.ddf.content

import com.adatao.spark.ddf.ATestSuite

/**
 */
class ListDDFSuite extends ATestSuite {
  createTableMtcars()
  createTableAirline()

  test("test list ddf") {
    val ddf1 = manager.sql2ddf("select * from mtcars")
    val ddf2 = manager.sql2ddf("select * from airline")

    manager.setDDFByName(ddf1, "mtcars")
    manager.addDDF(ddf2)

    val listDDF = manager.listDDFs()

    listDDF.foreach{
      ddfinfo => println(s">>> numRows = ${ddfinfo.getNumRows}; numColumns = ${ddfinfo.getNumColumns}\n " +
        s"uri = ${ddfinfo.getUri}; createdTime = ${ddfinfo.getCreatedTime}")
    }

    assert(listDDF.size == 1)
    assert(listDDF(0).getNumRows != null)
    assert(listDDF(0).getNumColumns != null)
    assert(listDDF(0).getUri != null)
  }
}
