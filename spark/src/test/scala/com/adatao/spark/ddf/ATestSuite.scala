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

  def createTableMtcars(sharkctx: SharkContext){
    sharkctx.sql("set shark.test.data.path=../resources")
    sharkctx.sql("drop table if exists mtcars")
    sharkctx.sql("CREATE TABLE mtcars ("
      + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
      + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
    sharkctx.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/mtcars' INTO TABLE mtcars")
  }

  def createTableAirline(sharkctx: SharkContext) {
    sharkctx.sql("set shark.test.data.path=../resources")
    sharkctx.sql("drop table if exists airline")
    sharkctx.sql("create table airline (Year int,Month int,DayofMonth int," +
        "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
        "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
        "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
        "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
        "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
        "CancellationCode string, Diverted string, CarrierDelay int, " +
        "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    sharkctx.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airline.csv' " +
      "INTO TABLE airline")
  }

  def createTableAirlineWithNA(sharkctx: SharkContext) {
    sharkctx.sql("set shark.test.data.path=../resources")
    sharkctx.sql("drop table if exists airline")
    sharkctx.sql("create table airline (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    sharkctx.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airlineWithNA.csv' " +
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
