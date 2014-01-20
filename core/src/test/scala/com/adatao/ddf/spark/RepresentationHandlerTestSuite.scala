package com.adatao.ddf.spark

import com.adatao.ddf.ATestSuite
import com.adatao.ddf.DDF
import org.apache.spark.SparkContext
import com.adatao.ddf.ARepresentationHandler
import org.scalatest.Assertions
import org.apache.spark.rdd.RDD

class RepresentationHandlerTestSuite extends ATestSuite {

	private var df: DDF = _
	private var helper: DDFHelper = _
	private var handler: RepresentationHandler = _
	private var list: Seq[Double] = _
	private var rdd: RDD[Double] = _

	override def beforeAll() {
		var sparkHome = System.getenv("SPARK_HOME")
		if (sparkHome == null) sparkHome = "/root/spark"

		val sc = new SparkContext("local", classOf[RepresentationHandlerTestSuite].getName(), sparkHome)
		list = List(1.0, 2, 3, 4)

		rdd = sc.parallelize(list)
		df = DDFHelper.newDDF(rdd)
		helper = df.getHelper().asInstanceOf[DDFHelper]
		handler = helper.getRepresentationHandler().asInstanceOf[RepresentationHandler]
	}

	test("Can instantiate a new DDF") {
		assert(df != null);
	}

	test("Can list DDF representations") {
		val list: String = df.getHelper().getRepresentationHandler().asInstanceOf[ARepresentationHandler].getList()
		println(list)
		assert(list.startsWith("1. key='class org.apache.spark.rdd.RDD[double]', value='ParallelCollectionRDD"))
	}

	test("Can represent DDFs as RDDs") {
		val elementType = rdd.take(1)(0).getClass()

		println(elementType)

		assert(null != handler.get(elementType), "There should now be a representation of type RDD[Double]")

		handler.set(rdd)
		assert(null != handler.get(elementType), "There should now be a representation of type RDD[Double]")

		handler.add(rdd)
		assert(null != handler.get(elementType), "There should now be a representation of type RDD[Double]")

		handler.remove(elementType)
		assert(null == handler.get(elementType), "There should now be no representation of type RDD[Double]")

		handler.reset()
		assert(null == handler.get(elementType), "There should not be any existing representations")
	}

}
