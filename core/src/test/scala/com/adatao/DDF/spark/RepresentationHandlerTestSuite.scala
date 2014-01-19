package com.adatao.DDF.spark

import com.adatao.DDF.ATestSuite
import com.adatao.DDF.DataFrame
import org.apache.spark.SparkContext

class RepresentationHandlerTestSuite extends ATestSuite {

	private var df: DataFrame = _

	override def beforeAll() {
		var sparkHome = System.getenv("SPARK_HOME")
		if (sparkHome == null) sparkHome = "/root/spark"

		val sc = new SparkContext("local", classOf[RepresentationHandlerTestSuite].getName(), sparkHome)
		val rdd = sc.parallelize(List(1, 2, 3, 4), 1)
		df = DataFrameImplementor.newDataFrame(rdd)
	}

	test("Can instantiate a new DataFrame") {
		assert(df != null);
	}

}
