package com.adatao.pa.spark.execution

import com.adatao.ML.AAlgorithmTest
import com.adatao.ML.spark.clustering.KMeansModel
import scala.collection.JavaConversions._
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.GetFactor.GetFactorResult
import java.util.Arrays
import io.ddf.ml.IModel
import org.apache.spark.mllib.linalg.DenseVector

/**
 */

class KmeansSuite extends ABigRClientTest {

  ignore("test Kmeans") {
    val numIters = 10
    val xCols = Array(0, 1)
    val K = 4
    val dataContainerID = this.loadFile(List("resources/KmeansTest.csv", "server/resources/KmeansTest.csv"), false, ",")
    println("dataContainerID = " + dataContainerID)
    val executor = new Kmeans(dataContainerID, xCols, numIters, K, null, "random")
    val r = bigRClient.execute[IModel](executor)
    assert(r.isSuccess)
    val model = r.result.getRawModel.asInstanceOf[KMeansModel]
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(-7.75, -8.25).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(-8.7, 6.75).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(7.5, 6.071428571428571).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(7.333333333333333, -8.0).deep))
  }

  test("test Kmeans on Shark") {
    createTableKmeans
    val loader = new Sql2DataFrame("select * from kmeans", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
    assert(r0.isSuccess)
    val dataContainerID = r0.dataContainerID
    val executor = new Kmeans(dataContainerID, Array(0, 1), 10, 4, null, "random")
    val r = bigRClient.execute[IModel](executor)
    assert(r.isSuccess)

    val model = r.result.getRawModel.asInstanceOf[KMeansModel]

    assert(model.clusterCenters.size == 4)
    model.clusterCenters.foreach{
      row => println(">>>>> clusterCenters = " + row.asInstanceOf[DenseVector].toArray.mkString(", "))
    }
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(-7.75, -8.25).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(-8.7, 6.75).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(7.5, 6.071428571428571).deep))
    assert(model.clusterCenters.exists(centers => centers.toArray.deep == Array(7.333333333333333, -8.0).deep))
    
    assert(model.wcss > 0)
  }
}
