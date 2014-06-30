package com.adatao.pa.spark.execution

import collection.JavaConversions._
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.NRow.NRowResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult

class BinningSuite extends ABigRClientTest {

  override def beforeAll = {
    super.beforeAll
    createTableMtcars
    createTableAirline
  }

  test("test empty bins") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)

    // test some empty bins
    // table(cut(mtcars$gear, breaks=c(2, 3.5, 4.5, 5.5, 6.0, 7.0, 10.0, 15.0)))
    // (2,3.5] (3.5,4.5] (4.5,5.5]   (5.5,6]     (6,7]    (7,10]   (10,15]
    // 15        12         5         0         0         0         0
    var cmd = new Binning(dcID, "gear", binningType = "custom", breaks = Array(2, 3.5, 4.5, 5.5, 6.0, 7.0, 10.0, 15.0))
    val binned = bigRClient.execute[BinningResult](cmd)

    val fetcher = new FetchRows().setDataContainerID(binned.result.dataContainerID).setLimit(32)
    val r2 = bigRClient.execute[FetchRowsResult](fetcher)
    assert(r2.isSuccess)

    var cmd2 = new GetMultiFactor(binned.result.dataContainerID, Array(9))
    val result = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd2).result

    val factor = result(0)._2
    println(">>>factor:" + factor)
    assert(factor.size === 3)
    assert(factor.get("(2,3.5]") === 15)
    assert(factor.get("(3.5,4.5]") === 12)
    assert(factor.get("(4.5,5.5]") === 5)
  }

  test("test right & includeLowest") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)

    // cut mpg default case
    // table(cut(mtcars$mpg, breaks=c(10.4, 15, 20, 25, 30, 33.9)))
    // (10.4,15]   (15,20]   (20,25]   (25,30] (30,33.9]
    // 4        12         8         2         4
    var cmd1 = new Binning(dcID, "mpg", binningType = "custom", breaks = Array(10.4, 15.0, 20.0, 25.0, 30.0, 33.9),
      includeLowest = false, right = true)
    var result = bigRClient.execute[BinningResult](cmd1)

    //    var col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    //    var factor = col.getFactor

    var cmd2 = new GetMultiFactor(result.result.dataContainerID, Array(0))
    var result3 = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd2).result
    var factor = result3(0)._2

    assert(factor.size === 5)
    assert(factor.get("(10.4,15]") === 4)
    assert(factor.get("(15,20]") === 12)
    assert(factor.get("(20,25]") === 8)
    assert(factor.get("(25,30]") === 2)
    assert(factor.get("(30,33.9]") === 4)

    // cut mgp, include lowest, lowest bin now has 6 elems
    cmd1 = new Binning(dcID, "mpg", binningType = "custom", breaks = Array(10.4, 15.0, 20.0, 25.0, 30.0, 33.9),
      includeLowest = true, right = true)
    result = bigRClient.execute[BinningResult](cmd1)

    //    col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    //    factor = col.getFactor

    var cmd3 = new GetMultiFactor(result.result.dataContainerID, Array(0))
    result3 = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd3).result
    factor = result3(0)._2

    assert(factor.size === 5)
    assert(factor.get("[10.4,15]") === 6)
    assert(factor.get("(15,20]") === 12)
    assert(factor.get("(20,25]") === 8)
    assert(factor.get("(25,30]") === 2)
    assert(factor.get("(30,33.9]") === 4)

    // cut mgp, right = false
    cmd1 = new Binning(dcID, "mpg", binningType = "custom", breaks = Array(10.4, 15.0, 20.0, 25.0, 30.0, 33.9),
      includeLowest = false, right = false)
    result = bigRClient.execute[BinningResult](cmd1)

  }

  test("test Binning with airline dataset for NAN handling") {
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    val cmd = new Binning(dcID, "v29", binningType = "custom", breaks = Array(0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0),
      includeLowest = true, right = false)
    val result = bigRClient.execute[BinningResult](cmd)

    val col = result.result.metaInfo.find(_.getHeader == "v29").getOrElse(fail())
    val factor = col.getFactor.toMap

    //      assert(factor === Map("[10,20)" -> 1, "[50,60)" -> 2, "[30,40)" -> 3, "[40,50)" -> 4, "[20,30)" -> 5, "[0,10)" -> 1, "[70,80]" -> 1))
  }

  test("test Binning with equal intervals") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    val cmd = new Binning(dcID, "mpg", binningType = "equalInterval", numBins = 10,
      includeLowest = false, right = false)
    val result = bigRClient.execute[BinningResult](cmd)

    var cmd3 = new GetMultiFactor(result.result.dataContainerID, Array(0))
    var result3 = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd3).result
    var factor = result3(0)._2

  }

  test("test Binning with equal frequency") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    val cmd = new Binning(dcID, "mpg", binningType = "equalFreq", numBins = 3, includeLowest = true)
    val result = bigRClient.execute[BinningResult](cmd)

    var cmd3 = new GetMultiFactor(result.result.dataContainerID, Array(0))
    var result3 = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd3).result
    var factor = result3(0)._2

  }

  test("test mutable Binning with equal intervals") {
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    
    //before binning
    val fetcher = new FetchRows().setDataContainerID(dcID).setLimit(3)
    val r = bigRClient.execute[FetchRowsResult](fetcher)
    assert(r.isSuccess)
    //assert(r.result.getData().get(0).split("\\t")(18) === "810")
    System.out.println(">>>>>OLD 1st ROW" + r.result.getData().get(0))
    
    //set mutable
    val cmd1 = new MutableDDF(dcID, true)
    val result1 = bigRClient.execute[Sql2DataFrameResult](cmd1)
    
    val cmd = new Binning(dcID, "v19", binningType = "equalInterval", numBins = 3,
      includeLowest = true, right = true)
    val result = bigRClient.execute[BinningResult](cmd)
    
    
    assert(result.result.dataContainerID == result1.result.dataContainerID)
    assert(result.result.dataContainerID == dcID)

    var cmd3 = new GetMultiFactor(result.result.dataContainerID, Array(0))
    var result3 = bigRClient.execute[Array[(Int, java.util.Map[String, java.lang.Integer])]](cmd3).result
    var factor = result3(0)._2
    
    
    val fetcher2 = new FetchRows().setDataContainerID(dcID).setLimit(3)
    val r2 = bigRClient.execute[FetchRowsResult](fetcher2)
    assert(r2.isSuccess)
    assert(r2.result.getData().get(0).split("\\t")(18) === "[162,869]")
        System.out.println(">>>>>NEW 1st ROW" + r2.result.getData().get(0))
  }

}
