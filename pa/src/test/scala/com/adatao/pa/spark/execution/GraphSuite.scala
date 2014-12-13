package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import scala.collection.JavaConversions._
import org.junit.Assert.assertEquals

/**
 * author: daoduchuan
 */
class GraphSuite extends ABigRClientTest {

  test("test graphtfidf") {
    createTableGraph
    val loader = new Sql2DataFrame("select * from graph", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
    val dataContainerID = r0.dataContainerID
    val cmd = new GraphTFIDF(dataContainerID, "source", "dest")
    val r = bigRClient.execute[DataFrameResult](cmd)
    val fetchRows = new FetchRows().setDataContainerID(r.result.dataContainerID).setLimit(10)
    val r2 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r2.result.getData

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => Array(arr(0), arr(1), arr(2).toDouble)}
    result.map{
      row => println(row.mkString(", "))
    }
    assert(result(0)(0) == "-3351804022671213993")
    assert(result(0)(1) == "-3351804022671214149")
    assert(result(0)(2) == 1.2148111519023101)

    assert(result(1)(0) == "-3351804022671213993")
    assert(result(1)(1) == "-3351804022671213881")
    assert(result(1)(2) == 0.20827853703164503)

    assert(result(2)(0) == "-3351804022671213785")
    assert(result(2)(1) == "-3351804022671220866")
    assert(result(2)(2) == 1.0123426265852582)

    assert(result(3)(0) == "-3351804022671213785")
    assert(result(3)(1) == "-3351804022671213881")
    assert(result(3)(2) == 0.3471308950527417)

    assert(result(5)(0) == "-3351804022671213759")
    assert(result(5)(1) == "-3351804022671217996")
    assert(result(5)(2) == 0.8282803308424841)
  }

  ignore("test Cosine Similarity") {
    createTableGraph1
    createTableGraph2
    val loader = new Sql2DataFrame("select * from graph1", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    val dataContainerID = r0.dataContainerID
    assert(r0.isSuccess)

    val loader2 = new Sql2DataFrame("select * from graph2", true)
    val r02 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader2).result
    val dataContainerID2 = r02.dataContainerID
    assert(r02.isSuccess)


    val cmd = new GraphTFIDF(dataContainerID, "source", "dest")
    val r = bigRClient.execute[DataFrameResult](cmd).result

    val cmd1 = new GraphTFIDF(dataContainerID2, "source", "dest")
    val r2 = bigRClient.execute[DataFrameResult](cmd1).result

    val cmd2 = new CosineSimilarity(r.dataContainerID, r2.dataContainerID, 0.5)
    val cosine = bigRClient.execute[DataFrameResult](cmd2).result

    assert(cosine.isSuccess)

    val fetchRowsCosine = new FetchRows().setDataContainerID(cosine.dataContainerID).setLimit(200)
    val fetchRowsCosineResult = bigRClient.execute[FetchRowsResult](fetchRowsCosine)
    val cosineResult = fetchRowsCosineResult.result.getData

    val cosineResult2 = cosineResult.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    assert(cosineResult2.size == 1)
    assert(cosineResult2(0)(0).asInstanceOf[String] == "SNA")
    assert(cosineResult2(0)(1).asInstanceOf[String] == "HCM")
    assertEquals(cosineResult2(0)(2).asInstanceOf[Double], 1, 0.1)

    /**
     * print out result from TFIDF and CosineSimilarity
     */
    println("#################################")
    println("########### TFIDF 1 ###############")
    val fetchRows = new FetchRows().setDataContainerID(r.dataContainerID).setLimit(200)
    val r4 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r4.result.getData

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    result.map{
      row => println(row.mkString(","))
    }

    println("#################################")
    println("########### TFIDF 2 ###############")
    val fetchRows2 = new FetchRows().setDataContainerID(r2.dataContainerID).setLimit(200)
    val r5 = bigRClient.execute[FetchRowsResult](fetchRows2)
    val ls1 = r5.result.getData

    val result1 = ls1.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    result1.map{
      row => println(row.mkString(","))
    }
    println("#################################")
    println("########### Cosine similarity ###############")
    cosineResult2.map{
      row => println(row.mkString(","))
    }
  }

  ignore("test Jaccard Similarity") {
    createTableGraph1
    createTableGraph2
    val loader = new Sql2DataFrame("select * from graph1", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    val dataContainerID = r0.dataContainerID
    assert(r0.isSuccess)

    val loader2 = new Sql2DataFrame("select * from graph2", true)
    val r02 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader2).result
    val dataContainerID2 = r02.dataContainerID
    assert(r02.isSuccess)


    val cmd = new GraphTFIDF(dataContainerID, "source", "dest")
    val r = bigRClient.execute[DataFrameResult](cmd).result

    val cmd1 = new GraphTFIDF(dataContainerID2, "source", "dest")
    val r2 = bigRClient.execute[DataFrameResult](cmd1).result

    val cmd2 = new JaccardSimilarity(r.dataContainerID, r2.dataContainerID, 0.0, 0.8)
    val jaccard = bigRClient.execute[DataFrameResult](cmd2).result

    assert(jaccard.isSuccess)

    val fetchRowsCosine = new FetchRows().setDataContainerID(jaccard.dataContainerID).setLimit(200)
    val fetchRowsCosineResult = bigRClient.execute[FetchRowsResult](fetchRowsCosine)
    val cosineResult = fetchRowsCosineResult.result.getData

    val cosineResult2 = cosineResult.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}


    assert(cosineResult2.size == 1)
//    assert(cosineResult2(0)(0).asInstanceOf[String] == "SNA")
//    assert(cosineResult2(0)(1).asInstanceOf[String] == "HCM")
//    assertEquals(cosineResult2(0)(2).asInstanceOf[Double], 1, 0.1)

    /**
     * print out result from TFIDF and CosineSimilarity
     */
    println("#################################")
    println("########### TFIDF 1 ###############")
    val fetchRows = new FetchRows().setDataContainerID(r.dataContainerID).setLimit(200)
    val r4 = bigRClient.execute[FetchRowsResult](fetchRows)
    val ls = r4.result.getData

    val result = ls.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    result.map{
      row => println(row.mkString(","))
    }

    println("#################################")
    println("########### TFIDF 2 ###############")
    val fetchRows2 = new FetchRows().setDataContainerID(r2.dataContainerID).setLimit(200)
    val r5 = bigRClient.execute[FetchRowsResult](fetchRows2)
    val ls1 = r5.result.getData

    val result1 = ls1.map {
      row => row.replace("\"", "").split("\\s+")
    }.map{arr => if(arr.size == 3) Array(arr(0), arr(1), arr(2).toDouble) else Array()}

    result1.map{
      row => println(row.mkString(","))
    }
    println("#################################")
    println("########### Cosine similarity ###############")
    cosineResult2.map{
      row => println(row.mkString(","))
    }

  }
}
