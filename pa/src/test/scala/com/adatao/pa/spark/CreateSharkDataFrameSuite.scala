package com.adatao.pa.spark

import java.util.{HashMap => JMap}

import org.junit.runner.RunWith


import com.adatao.pa.spark.execution.CVKFoldSplit
import com.adatao.pa.spark.execution.Kmeans
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.pa.spark.execution.CVRandomSplit
import com.adatao.pa.spark.execution.LinearRegressionNormalEquation
import com.adatao.pa.spark.execution.MapReduceNative
import com.adatao.pa.spark.execution.Sql2DataFrame
import com.adatao.pa.spark.execution.FiveNumSummary
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import com.adatao.pa.spark.execution.GetMultiFactor
import com.adatao.pa.spark.execution.TransformNativeRserve
import com.adatao.pa.spark.execution.YtrueYpred
import com.adatao.pa.spark.execution.SampleDataFrame
import com.adatao.pa.spark.execution.SampleDataFrame.SampleDataFramePercentResult
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult
/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 5/12/13
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
class CreateSharkDataFrameSuite extends ABigRClientTest{

	test("test CVRandomSplit"){

		createTableMtcars
		val df = this.runSQL2RDDCmd("select * from mtcars", true)
		val dataContainerId = df.dataContainerID

		val splitter = new CVRandomSplit(dataContainerId, 2, 0.75, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 2)

		r.result(0) match{
			case Array(train, test) => {

				val cmd= new FiveNumSummary(train)
				val res= bigRClient.execute[Array[ASummary]](cmd)

				assert(res.isSuccess)
				val cmd2= new FiveNumSummary(test)
				val res2= bigRClient.execute[Array[ASummary]](cmd2)
				assert(res2.isSuccess)
			}
		}

	}

	test("test CVFoldSplit") {

    createTableMtcars
		val df = this.runSQL2RDDCmd("select * from mtcars", true)
		val dcID = df.dataContainerID


		val splitter= new CVKFoldSplit(dcID, 5, 42)
		val r= bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 5)

		r.result(0) match{
			case Array(train, test) => {
				val cmd= new FiveNumSummary(train)
				val res= bigRClient.execute[Array[ASummary]](cmd)
				assert(res.isSuccess)
				val cmd2= new FiveNumSummary(test)
				val res2= bigRClient.execute[Array[ASummary]](cmd)
				assert(res2.isSuccess)
			}
		}
	}

	test("test SampleDataFrame"){

		createTableMtcars
		val df = this.runSQL2RDDCmd("select * from mtcars", true)
		val dcID = df.dataContainerID

		val cmd= new SampleDataFrame().setDataContainerID(dcID).setPercent(0.5).setReplace(false).setGetPercent(true)
		val res= bigRClient.execute[SampleDataFramePercentResult](cmd)
		assert(res.isSuccess == true)
		LOG.info("datacontainerID= " + dcID)
		val dcID2= res.result.getDataContainerID
		val cmd2= new GetMultiFactor(dcID2, Array(0,1,2,3,4,5))

		println(">>>>>>>>>>>>>>> dcID2 = " + dcID2)

		val result= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd2)
		assert(result.isSuccess)
	}
//	test("test Kmeans prediction") {
//		createTableKmeans
//		
//		val df = this.runSQL2RDDCmd("select * from kmeans", true)
//    val dcID = df.dataContainerID
//
//		val executor= new Kmeans(dcID, Array(0,1), 5, 4, null, "random")
//		val r1= bigRClient.execute[KmeansModel](executor)
//		assert(r1.isSuccess)
//
//		val executor1= new XsYpred(dcID, r1.persistenceID, Array(0,1))
//		val r2= bigRClient.execute[XsYpredResult](executor1)
//		assert(r2.isSuccess)
//		val dcID2= r2.result.dataContainerID
//
//		val executor2= new FiveNumSummary(dcID2)
//		val r3= bigRClient.execute[Array[ASummary]](executor2)
//		assert(r3.isSuccess)
//		val executor22= new GetMultiFactor(dcID2, Array(0,1,2))
//		val r4= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](executor22)
//		assert(r4.isSuccess)
//	}
//	test("test YtrueYpred "){
//		createTableKmeans
//		val loader = new Sql2DataFrame("select * from kmeans", true)
//		val r0= bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
//		val dcID= r0.dataContainerID
//
//		val cmd= new LinearRegressionNormalEquation(dcID, Array(0), 1, 0.0)
//		val r= bigRClient.execute[NQLinearRegressionModel](cmd)
//
//		val cmd1= new YtrueYpred(dcID, r.persistenceID, Array(0), 1)
//		val r1= bigRClient.execute[YtrueYpredResult](cmd1)
//		assert(r1.isSuccess)
//
//		val cmd2= new FiveNumSummary(r1.result.dataContainerID)
//		val r2= bigRClient.execute[Array[ASummary]](cmd2)
//		assert(r2.isSuccess)
//
//		val cmd3= new GetMultiFactor(r1.result.dataContainerID, Array(0,1))
//		val r3= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd3)
//		assert(r3.isSuccess)
//
//	}

//	test("test TransformNativeRserve") {
//		createTableMtcars
//    val df = this.runSQL2RDDCmd("select mpg, gear from mtcars", true)
//
//		val dataContainerId = df.dataContainerID
//
//		val transformer = new TransformNativeRserve(dataContainerId, "newcol = mpg / gear")
//		val r1 = bigRClient.execute[DataFrameResult](transformer)
//		assert(r1.isSuccess)


//		val cmd2= new FiveNumSummary(r1.result.dataContainerID)
//		val r2= bigRClient.execute[Array[ASummary]](cmd2)
//		assert(r2.isSuccess)
//
//		val cmd3= new GetMultiFactor(r1.result.dataContainerID, Array(0,1))
//		val r3= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd3)
//		assert(r3.isSuccess)
//	}


	test("test MapReduceNative") {
		createTableMtcars
    val df = this.runSQL2RDDCmd("select * from mtcars", true)

		val dataContainerId = df.dataContainerID

		// aggregate sum of hp group by gear
		val mr = new MapReduceNative(dataContainerId,
      "function(part) { keyval(key=part$gear, val=part$hp) }",
      "function(key, vv) { keyval.row(key=key, val=sum(vv)) }")
		val r1 = bigRClient.execute[DataFrameResult](mr)

		assert(r1.isSuccess)

		val cmd2= new FiveNumSummary(r1.result.dataContainerID)
		val r2= bigRClient.execute[Array[ASummary]](cmd2)
		assert(r2.isSuccess)

		val cmd3= new GetMultiFactor(r1.result.dataContainerID, Array(0,1))
		val r3= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd3)
		assert(r3.isSuccess)
	}
}
