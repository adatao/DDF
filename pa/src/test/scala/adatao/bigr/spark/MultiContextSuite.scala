package adatao.bigr.spark

import adatao.bigr.spark.types.BigRThriftServerUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import adatao.bigr.spark.execution.LoadTable
import adatao.ML.TTestCanLog
import adatao.bigr.spark.types.BigRClientTestUtils
import adatao.bigr.spark.types.BigRClient
import adatao.ML.ATestSuite
import adatao.bigr.thrift.Server

/*
class MultiContextSuite extends ATestSuite {
	test("Test MultiUser implementation using multiple thrift server"){
		val prevMultiUser = System.getProperty("bigr.multiuser", Server.MULTIUSER_DEFAULT.toString);
		System.setProperty("bigr.multiuser", "true")
		val client1 = BigRThriftServerUtils.startServer
		assert(client1 != null)
		client1.connect()
			
		val client2 = BigRThriftServerUtils.createClient
		assert(client2 != null)
		client2.connect("{clientID=user2}")

		BigRClientTestUtils.createTableAirline(client1)
		var df = BigRClientTestUtils.runSQL2RDDCmd(client1, "SELECT * FROM airline", true)
		assert(df.isSuccess)
		var dcID = df.dataContainerID
		LOG.info("Got dataConainterID = " + dcID)
		client1.disconnect

		
		BigRClientTestUtils.createTableAirline(client2)
		df = BigRClientTestUtils.runSQL2RDDCmd(client2, "SELECT * FROM airline", true)
		assert(df.isSuccess)
		dcID = df.dataContainerID
		LOG.info("Got dataConainterID = " + dcID)
		client2.disconnect

		BigRThriftServerUtils.stopServer
		System.setProperty("bigr.multiuser", prevMultiUser);
	}
}*/
