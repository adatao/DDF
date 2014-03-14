package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.HadoopFsShell._

class HadoopFsShellSuite extends ABigRClientTest {
	test("test HadoopFsShell") {
		// Test ls
		val cmd1 = new HadoopFsShell("ls", "/")
		val res1 = bigRClient.execute[HadoopFsShellResult](cmd1)
		assert(res1.isSuccess == true)
		println(res1.result.getResult())
		// Output: Found xxx items
		assert(res1.result.getResult().indexOf("items") > 0)
	}
}