package com.adatao.pa.spark.execution

import com.adatao.ML.{KmeansModel, AAlgorithmTest}
import scala.collection.JavaConversions._
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.GetFactor.GetFactorResult
import java.util.Arrays
/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 8/8/13
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */
class KmeansSuite extends ABigRClientTest{

	test("test Kmeans") {
		val numIters = 10
		val xCols = Array(0, 1)
		val K = 4
		val dataContainerID = this.loadFile(List("resources/KmeansTest.csv", "server/resources/KmeansTest.csv"), false, ",")
		val executor = new Kmeans(dataContainerID, xCols, numIters, K, null, "random")
		val r = bigRClient.execute[KmeansModel](executor)
		assert(r.isSuccess)
		val model = r.result

		assert(model.pointsPerCluster.deep == Array(8, 10, 14, 9).deep)
		assert(model.centroids(0).deep == Array(-7.75, -8.25).deep)
		assert(model.centroids(1).deep == Array(-8.7, 6.75).deep)
		assert(model.centroids(2).deep == Array(7.5, 6.071428571428571).deep)
		assert(model.centroids(3).deep == Array(7.333333333333333,-8.0).deep)
		assert(model.totalWithins.deep ==
		Array(3697.25,869.0478316326531,289.1535714285714,
				289.1535714285714,289.1535714285714,
				289.1535714285714,289.1535714285714,
				289.1535714285714,289.1535714285714,
				289.1535714285714).deep)
	}
	
	test("test Kmeans on Shark"){
		createTableKmeans
		val loader = new Sql2DataFrame("select * from kmeans", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)
		val dataContainerID = r0.dataContainerID
		val executor = new Kmeans(dataContainerID, Array(0, 1), 10, 4, initialCentroids = null, "random")
		val r = bigRClient.execute[KmeansModel](executor)
		assert(r.isSuccess)

		val model = r.result

		assert(model.pointsPerCluster.deep == Array(8, 10, 14, 9).deep)
		assert(model.centroids(0).deep == Array(-7.75, -8.25).deep)
		assert(model.centroids(1).deep == Array(-8.7, 6.75).deep)
		assert(model.centroids(2).deep == Array(7.5, 6.071428571428571).deep)
		assert(model.centroids(3).deep == Array(7.333333333333333,-8.0).deep)
		assert(model.totalWithins.deep ==
									Array(3697.25,869.0478316326531,289.1535714285714,
									289.1535714285714,289.1535714285714,
									289.1535714285714,289.1535714285714,
									289.1535714285714,289.1535714285714,
									289.1535714285714).deep)
	}

	test("test NAN handling"){
		assert(runSQLCmd("set shark.test.data.path=" + "resources").isSuccess)
		assert(runSQLCmd("drop table if exists kmeans").isSuccess)
		assert(runSQLCmd("CREATE TABLE kmeans (v1 double, v2 double ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").isSuccess)
		assert(runSQLCmd("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/KmeansTest1.csv' INTO TABLE kmeans").isSuccess)

		val loader = new Sql2DataFrame("select * from kmeans", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)
		val dataContainerID = r0.dataContainerID
		val executor = new Kmeans(dataContainerID, Array(0,1), 10, 4, null, "random")
		val r = bigRClient.execute[KmeansModel](executor)
		assert(r.isSuccess)
		assert(r.result.numSamples == 39)

		val executor1 = new XsYpred(dataContainerID, r.persistenceID, Array(0,1))
		val r1 = bigRClient.execute[XsYpredResult](executor1)
		assert(r1.isSuccess)

		val dataContainerID1 = this.loadFile(List("resources/KmeansTest1.csv", "server/resources/KmeansTest1.csv"), false, ",")
		val executor2 = new Kmeans(dataContainerID1, Array(0, 1), 10, 4, Arrays.asList(Array(6.0,7.0), Array(-1.0, 2), Array(0.0, 0.0), Array(1.0, 2.0)), "random")
		val r2 = bigRClient.execute[KmeansModel](executor2)
		assert(r2.isSuccess)
		assert(r2.result.numSamples == 39)
		assert(r2.result.pointsPerCluster.deep == Array(14, 10, 7, 8).deep)
		assert(r2.result.centroids(0).deep == Array(7.5,6.071428571428571).deep)


		val executor3 = new XsYpred(dataContainerID1, r2.persistenceID, Array(0,1))
		val r3 = bigRClient.execute[XsYpredResult](executor3)
		assert(r3.isSuccess)
	}

}
