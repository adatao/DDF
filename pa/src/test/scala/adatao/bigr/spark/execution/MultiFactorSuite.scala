package adatao.bigr.spark.execution

import adatao.bigr.spark.types.ABigRClientTest

//import scala.collection.JavaConversions._

import java.util.{HashMap => JMap}

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 23/9/13
 * Time: 9:01 PM
 * To change this template use File | Settings | File Templates.
 */
class MultiFactorSuite extends ABigRClientTest {

  test("test Shark") {
    createTableMtcars
    val df = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID= " + dcID)
    val cmd = new GetMultiFactor(dcID, Array(7, 8, 9, 10))
    val result = bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd).result

    assert(result(0)._2.get("1") == 14)
    assert(result(0)._2.get("0") == 18)
    assert(result(1)._2.get("1") == 13)
    assert(result(2)._2.get("4") == 12)
    assert(result(2)._2.get("3") == 15)
    assert(result(2)._2.get("5") == 5)
    assert(result(3)._2.get("1") == 7)
    assert(result(3)._2.get("2") == 10)
    assert(result(3)._2.get("8") == 1)
  }

  test("test DataFrame") {
    val dcID = this.loadFile(List("resources/mtcars", "server/resources/KmeansTest.csv"), false, " ")
    //assert(df.isSuccess)
    //val dcID = df.dataContainerID
    //LOG.info("Got dataContainerID= " + dcID)
    val cmd = new GetMultiFactor(dcID, Array(7, 8, 9, 10))
    val result = bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd).result

    assert(result(0)._2.get("1.0") == 14.0)
    assert(result(0)._2.get("0.0") == 18.0)
    assert(result(1)._2.get("1.0") == 13.0)
    assert(result(2)._2.get("4.0") == 12.0)
    assert(result(2)._2.get("3.0") == 15.0)
    assert(result(2)._2.get("5.0") == 5.0)
    assert(result(3)._2.get("1.0") == 7.0)
    assert(result(3)._2.get("2.0") == 10.0)
    assert(result(3)._2.get("8.0") == 1.0)
  }
  test("test NA handling") {
    createTableAirlineWithNA
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID= " + dcID)
    val cmd = new GetMultiFactor(dcID, Array(0, 8, 16, 17, 24, 25))
    val result = bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd).result
    assert(result(0)._2.get("2008") == 28.0)
    assert(result(0)._2.get("2010") == 1.0)
    assert(result(1)._2.get("WN") == 28.0)
    assert(result(2)._2.get("ISP") == 12.0)
    assert(result(2)._2.get("IAD") == 2.0)
    assert(result(2)._2.get("IND") == 17.0)
    assert(result(3)._2.get("MCO") == 3.0)
    assert(result(3)._2.get("TPA") == 3.0)
    assert(result(3)._2.get("JAX") == 1.0)
    assert(result(3)._2.get("LAS") == 3.0)
    assert(result(3)._2.get("BWI") == 10.0)
    assert(result(5)._2.get("0.0") == 9.0)
    assert(result(4)._2.get("3.0") == 1.0)

    val dcID2 = this.loadFile(List("resources/airlineWithNa.csv", "server/resources/airlineWithNa.csv"), false, ",")
    val cmd2 = new GetMultiFactor(dcID2, Array(0, 8, 16, 17, 24, 25))
    val result2 = bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd2).result
    assert(result2(0)._2.get("2008.0") == 28.0)
    assert(result2(0)._2.get("2010.0") == 1.0)

    assert(result2(1)._2.get("WN") == 28.0)
    assert(result2(2)._2.get("ISP") == 12.0)
    assert(result2(2)._2.get("IAD") == 2.0)
    assert(result2(2)._2.get("IND") == 17.0)
    assert(result2(3)._2.get("MCO") == 3.0)
    assert(result2(3)._2.get("TPA") == 3.0)
    assert(result2(3)._2.get("JAX") == 1.0)
    assert(result2(3)._2.get("LAS") == 3.0)
    assert(result2(3)._2.get("BWI") == 10.0)
    assert(result(5)._2.get("0.0") == 9.0)
    assert(result(4)._2.get("3.0") == 1.0)
  }

  test("test NA handling index out of bound") {
    createTableAirlineWithNA
    val df = this.runSQL2RDDCmd("SELECT * FROM airline", true)
    assert(df.isSuccess)
    val dcID = df.dataContainerID
    LOG.info("Got dataContainerID= " + dcID)
    val cmd = new GetMultiFactor(dcID, Array(0, 8, 16, 17, 24, 25, 10000))
    try {
      val result = bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd)
    }
    catch {
      case e =>
    }
  }
}
