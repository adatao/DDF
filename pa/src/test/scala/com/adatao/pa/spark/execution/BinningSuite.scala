package com.adatao.pa.spark.execution

import collection.JavaConversions._
import com.adatao.pa.spark.types.ABigRClientTest

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
    val cmd = new Binning(dcID, "gear", binningType = "custom", breaks = Array(2, 3.5, 4.5, 5.5, 6.0, 7.0, 10.0, 15.0))
    val binned = bigRClient.execute[BinningResult](cmd)
    binned.result.metaInfo.find(_.getHeader == "gear").map { col =>
      val factor = col.getFactor
      assert(factor.size === 3)
      assert(factor.get("(2,3.5]") === 15)
      assert(factor.get("(3.5,4.5]") === 12)
      assert(factor.get("(4.5,5.5]") === 5)
    }
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
    var col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    var factor = col.getFactor
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
    col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    factor = col.getFactor
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
    col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    factor = col.getFactor
    assert(factor.size === 5)
    assert(factor.get("[10.4,15)") === 5)
    assert(factor.get("[15,20)") === 13)
    assert(factor.get("[20,25)") === 8)
    assert(factor.get("[25,30)") === 2)
    assert(factor.get("[30,33.9)") === 3)

    // cut mgp, right = false, includeLowest = true, top bin count changed
    cmd1 = new Binning(dcID, "mpg", binningType = "custom", breaks = Array(10.4, 15.0, 20.0, 25.0, 30.0, 33.9),
      includeLowest = true, right = false)
    result = bigRClient.execute[BinningResult](cmd1)
    col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    factor = col.getFactor
    assert(factor.size === 5)
    assert(factor.get("[10.4,15)") === 5)
    assert(factor.get("[15,20)") === 13)
    assert(factor.get("[20,25)") === 8)
    assert(factor.get("[25,30)") === 2)
    assert(factor.get("[30,33.9]") === 4)
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
    assert(factor === Map("[10,20)" -> 3, "[50,60)" -> 1, "[30,40)" -> 1, "[40,50)" -> 1, "[20,30)" -> 1, "[0,10)" -> 1, "[70,80]" -> 1))
  }

  test("test Binning with equal intervals") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    val cmd = new Binning(dcID, "mpg", binningType = "equalInterval", numBins = 10,
      includeLowest = false, right = false)
    val result = bigRClient.execute[BinningResult](cmd)
    val col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    val factor = col.getFactor.toMap
    assert(factor === Map(
      "[10.4,12.75)" -> 2,
      "[12.75,15.1)" -> 4,
      "[15.1,17.45)" -> 6,
      "[17.45,19.8)" -> 6,
      "[19.8,22.15)" -> 5,
      "[22.15,24.5)" -> 3,
      "[24.5,26.85)" -> 1,
      "[26.85,29.2)" -> 1,
      "[29.2,31.55)" -> 2,
      "[31.55,33.9)" -> 1))
  }

  test("test Binning with equal frequency") {
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID = " + dcID)
    val cmd = new Binning(dcID, "mpg", binningType = "equalFreq", numBins = 3, includeLowest = true)
    val result = bigRClient.execute[BinningResult](cmd)
    val col = result.result.metaInfo.find(_.getHeader == "mpg").getOrElse(fail())
    val factor = col.getFactor.toMap
    assert(factor === Map("[10.4,16.2]" -> 10, "(16.2,21.4]" -> 12, "(21.4,33.9]" -> 10))
  }

}
