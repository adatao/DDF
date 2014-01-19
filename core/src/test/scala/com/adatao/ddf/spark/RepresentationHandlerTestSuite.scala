package com.adatao.ddf.spark

import com.adatao.ddf.ATestSuite
import com.adatao.ddf.DDF
import org.apache.spark.SparkContext
import com.adatao.ddf.ARepresentationHandler

class RepresentationHandlerTestSuite extends ATestSuite {

	private var df: DDF = _

	override def beforeAll() {
		var sparkHome = System.getenv("SPARK_HOME")
		if (sparkHome == null) sparkHome = "/root/spark"

		val sc = new SparkContext("local", classOf[RepresentationHandlerTestSuite].getName(), sparkHome)
		val rdd = sc.parallelize(List(1.0, 2, 3, 4), 1)
		df = DDFHelper.newDDF(rdd)
	}

	test("Can instantiate a new DDF") {
		assert(df != null);
	}
	
	test("Can list DDF representations") {
		val list: String = df.getHelper().getRepresentationHandler().asInstanceOf[ARepresentationHandler].getList()
		assert(list.startsWith("1. key='class org.apache.spark.rdd.RDD[double]', value='ParallelCollectionRDD[0]"))
	}

}
