package com.adatao.ddf.spark.content

import com.adatao.ddf.ATestSuite
import com.adatao.ddf.DDF
import org.apache.spark.SparkContext
import com.adatao.ddf.content.ARepresentationHandler
import org.apache.spark.rdd.RDD
import com.adatao.ddf.spark.DDFHelper

/**
 * Spark-specific RepresentationHandler tests
 */
class RepresentationHandlerTestSuite extends ATestSuite {

	private var df: DDF = _
	private var helper: DDFHelper = _
	private var rep: RepresentationHandler = _
	private var list: Seq[Double] = _
	private var sc: SparkContext = _
	private var rdd: RDD[Double] = _

	override def beforeAll() {
		var sparkHome = System.getenv("SPARK_HOME")
		if (sparkHome == null) sparkHome = "/root/spark"

		sc = new SparkContext("local", classOf[RepresentationHandlerTestSuite].getName(), sparkHome)
		list = List(1.0, 2, 3, 4)

		rdd = sc.parallelize(list)
		df = DDFHelper.newDDF(rdd)
		helper = df.getHelper().asInstanceOf[DDFHelper]
		rep = helper.getRepresentationHandler().asInstanceOf[RepresentationHandler]
	}

	override def afterAll() {
		sc.stop()
	}

	test("Can instantiate a new DDF") {
		assert(df != null);
	}

	test("Can list DDF representations") {
		val list: String = df.getHelper().getRepresentationHandler().asInstanceOf[ARepresentationHandler].getList()
		//println(list)
		assert(list.startsWith("1. key='class org.apache.spark.rdd.RDD[double]', value='ParallelCollectionRDD"))
	}

	test("Can represent DDFs as RDDs") {
		val elementType = rdd.take(1)(0).getClass()

		//println(elementType)

		assert(null != rep.get(elementType), "There should already be a representation of type RDD[Double] in ddf")

		rep.set(rdd)
		assert(null != rep.get(elementType), "There should now be a representation of type RDD[Double] after a set()")

		rep.add(rdd)
		assert(null != rep.get(elementType), "There should now be a representation of type RDD[Double] after an add()")

		rep.remove(elementType)
		assert(null == rep.get(elementType), "There should now be no representation of type RDD[Double] after this remove()")

		rep.reset()
		assert(null == rep.get(elementType), "There should not be any existing representations after a reset()")
	}

}
