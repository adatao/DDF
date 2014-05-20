package com.adatao.pa.spark

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution._
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import java.util.{HashMap => JMap}
import scala.collection.JavaConverters._
import com.adatao.pa.spark.execution.SampleDataFrame.SampleDataFramePercentResult
import org.junit.Assert.assertEquals
import com.adatao.ML.KmeansModel

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 5/12/13
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
class CreateSharkDataFrameSuite extends ABigRClientTest{

	test("test CVRandomSplit"){
//		val dataContainerId = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
		
		createTableAirline
		val df = this.runSQL2RDDCmd("select * from airline", true)
		val dataContainerId = df.dataContainerID
		
		val splitter = new CVRandomSplit(dataContainerId, 3, 0.75, 42)
		val r = bigRClient.execute[Array[Array[String]]](splitter)
		assert(r.isSuccess)
		println(r.result)
		assert(r.result.length === 3)

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

	test("test CVFoldSplit") {
//		val dcID = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")
		
		createTableAirline
		val df = this.runSQL2RDDCmd("select * from airline", true)
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
//		val dcID = this.loadFile("resources/mtcars", false, " ")
		
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
//		val loader = new Sql2DataFrame("select * from kmeans", true)
//		val r= bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
//		val dcID= r.dataContainerID
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
	/*
	test("test TransformNativeRserve") {
		createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val transformer = new TransformNativeRserve(dataContainerId, "newcol = mpg / gear")
		val r1 = bigRClient.execute[DataFrameResult](transformer)
		assert(r1.isSuccess)

		val cmd2= new FiveNumSummary(r1.result.dataContainerID)
		val r2= bigRClient.execute[Array[ASummary]](cmd2)
		assert(r2.isSuccess)

		val cmd3= new GetMultiFactor(r1.result.dataContainerID, Array(0,1))
		val r3= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd3)
		assert(r3.isSuccess)
	}  */
	/*
	test("test MapReduceNative") {
		createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		// aggregate sum of hp group by gear
		val mr = new MapReduceNative(dataContainerId,
			mapFuncDef = "function(part) { keyval(key=part$gear, val=part$hp) }",
			reduceFuncDef = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }")
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		val cmd2= new FiveNumSummary(r1.result.dataContainerID)
		val r2= bigRClient.execute[Array[ASummary]](cmd2)
		assert(r2.isSuccess)

		val cmd3= new GetMultiFactor(r1.result.dataContainerID, Array(0,1))
		val r3= bigRClient.execute[Array[(Int, JMap[String, java.lang.Integer])]](cmd3)
		assert(r3.isSuccess)
	}   */
}
