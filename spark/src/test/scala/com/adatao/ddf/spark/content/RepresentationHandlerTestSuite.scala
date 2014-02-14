
package com.adatao.ddf.spark.content

import com.adatao.ddf.spark.{SparkDDFManager, ATestSuite, DDFHelper}
import com.adatao.ddf.{ATestSuite, ADDFHelper, DDF}
import org.apache.spark.SparkContext
import com.adatao.ddf.content.{AMetaDataHandler, ARepresentationHandler}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.slf4j.{LoggerFactory, Logger}
import shark.{SharkEnv, SharkContext}
import com.adatao.ddf.spark.etl.SparkPersistenceHandler
import com.adatao.ddf.content.Schema
import com.adatao.ddf.spark.analytics.{KmeansModel, KmeansParameters, KmeansRunner}

/**
 * Spark-specific SparkRepresentationHandler tests
 */
class RepresentationHandlerTestSuite extends ATestSuite {

	private var df: DDF = _
	private var manager: SparkDDFManager = _
	private var rep: SparkRepresentationHandler = _
	private var list: Seq[Double] = _
	private var sc: SparkContext = _
	private var sharkctx: SharkContext= _
  private var rdd: RDD[Double] = _

	override def beforeAll() {
		var sparkHome = System.getenv("SPARK_HOME")
		if (sparkHome == null) sparkHome = "/root/spark"

		//sc = new SparkContext("local", classOf[RepresentationHandlerTestSuite].getName(), sparkHome)
    sc= SharkEnv.initWithSharkContext(new SharkContext("local",
      classOf[RepresentationHandlerTestSuite].getName, sparkHome, Nil, Map()))
    sharkctx= sc.asInstanceOf[SharkContext]

    list = List(1.0, 2, 3, 4)

		rdd = sc.parallelize(list)
		//df = DDM.newDDF(rdd)
		//helper = df.getHelper().asInstanceOf[DDFHelper]
		rep = manager.getRepresentationHandler().asInstanceOf[SparkRepresentationHandler]
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
		assert(list.startsWith("1. key='double', value='ParallelCollectionRDD"))
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

  test("Can get Array[Object] representation") {
    createTableMtcars(sharkctx)

    //create new empty DDF
    val ddf: DDF= new DDF(new SparkDDFManager())

    val helper: SparkDDFManager= ddf.getDDFManager.asInstanceOf[SparkDDFManager]

    helper.setMetaDataHandler(new MetaDataHandler(helper))
    helper.getMetaDataHandler.setSchema(new Schema("shark_mtcars"))

    val schema= helper.getMetaDataHandler.getSchema

    val persHandler= helper.getPersistenceHandler.asInstanceOf[SparkPersistenceHandler]

    persHandler.sharkContext= sharkctx

    val viewHandler= helper.getViewHandler

    //val schemaHandler= helper.getSchemaHandler.asInstanceOf[SparkSchemaHandler]


    val subsetcmd= "SELECT * FROM mtcars"
    val cmd= String.format("CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=" +
      "\"MEMORY_ONLY\") AS %s", schema.getTableName, subsetcmd)

    persHandler.sql("drop table if exists " + schema.getTableName)

    //Load DDF with Shark's query
    persHandler.sqlLoad(null, cmd)

    //get new Representation
    val repHandler= helper.getRepresentationHandler

    repHandler.getRepresentation(classOf[Array[Object]])

    val obj= repHandler.get(classOf[Array[Object]])
    assert(obj != null)

    val rdd= obj.asInstanceOf[RDD[Array[Object]]]

    assert(rdd != null)
    assert(rdd.count == 32)

  }
  test("Can get Array[Double] representation") {
    createTableMtcars(sharkctx)

    //create new empty DDF
    val ddf: DDF= new DDF(new DDFHelper(null))

    val helper: DDFHelper= ddf.getHelper.asInstanceOf[DDFHelper]

    helper.setMetaDataHandler(new MetaDataHandler(helper))
    helper.getMetaDataHandler.setSchema(new Schema("shark_mtcars"))

    val schema= helper.getMetaDataHandler.getSchema
    val persHandler= helper.getPersistenceHandler.asInstanceOf[SparkPersistenceHandler]

    persHandler.sharkContext= sharkctx

    val subsetcmd= "SELECT * FROM mtcars"
    val cmd= String.format("CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=" +
      "\"MEMORY_ONLY\") AS %s", schema.getTableName, subsetcmd)

    persHandler.sql(null, "drop table if exists " + schema.getTableName)

    //Load DDF with Shark's query
    persHandler.sqlLoad("", cmd)

    //get new Representation
    val repHandler= helper.getRepresentationHandler

    repHandler.getRepresentation(classOf[Array[Double]])

    val obj= repHandler.get(classOf[Array[Double]])
    assert(obj != null)

    val rdd= obj.asInstanceOf[RDD[Array[Double]]]

    assert(rdd != null)
    assert(rdd.count === 32)
    assert(rdd.first().size === 11)
    assert(rdd.first.deep == Array(21.0, 6.0, 160, 110, 3.90, 2.620, 16.46, 0.0, 1.0, 4.0, 4.0).deep)
  }
}

//Dummy class to get the test going
class MetaDataHandler(container: ADDFHelper) extends AMetaDataHandler(container){

  override def getNumRowsImpl(): Long = {
    9L
  }
}
