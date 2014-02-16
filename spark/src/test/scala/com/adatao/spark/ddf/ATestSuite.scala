// scalastyle:off
package com.adatao.spark.ddf

import org.scalatest.FunSuite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterEach
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll
import shark.SharkContext

/**
 * This makes a Logger LOG variable available to the test suite.
 * Also makes beforeEach/afterEach as well as beforeAll/afterAll behaviors available.
 */
@RunWith(classOf[JUnitRunner])
abstract class ATestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass())

  def createTableMtcars(sharksc: SharkContext){

    sharksc.sql("set shark.test.data.path=../resources")
    sharksc.sql("drop table if exists mtcars")
    sharksc.sql("CREATE TABLE mtcars ("
      + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
      + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
    sharksc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/tests/mtcars' INTO TABLE mtcars")
  }

  def createTableAirline(sharksc: SharkContext) {

    sharksc.sql("set shark.test.data.path=../resources")
    sharksc.sql("drop table if exists airline")
    sharksc.sql("create table airline (v1 int, v2 double, v3 double, v4 double," +
      " v5 double, v6 double, v7 double, v8 double, v9 string, v10 double," +
      " v11 string, v12 double, v13 double, v14 double, v15 double, v16 double, " +
      "v17 string, v18 string, v19 double, v20 double, v21 double, v22 double, v23" +
      " double, v24 double, v25 double, v26 double, v27 double, v28 double, v29 double)" +
      " row format delimited fields terminated by ','")
    sharksc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/tests/airline.csv' " +
      "INTO TABLE airline")
  }
}

/**
 * This logs the begin/end of each test with timestamps and test #
 */
abstract class ATimedTestSuite extends ATestSuite {
  private lazy val testNameArray: Array[String] = testNames.toArray
  private var testNumber: Int = 0
  def getCurrentTestName = "Test #%d: %s".format(testNumber + 1, testNameArray(testNumber))

  override def beforeEach = {
    LOG.info("%s started".format(this.getCurrentTestName))
  }

  override def afterEach = {
    testNumber += 1
    LOG.info("%s ended".format(this.getCurrentTestName))
    super.afterEach
  }
}
