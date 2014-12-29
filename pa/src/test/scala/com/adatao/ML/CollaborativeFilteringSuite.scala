package com.adatao.ML

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import scala.collection.JavaConversions._
import com.adatao.pa.spark.types.ABigRClientTest
import java.util.Arrays
import io.ddf.ml.IModel
import com.adatao.pa.spark.execution.ALS
import com.adatao.pa.spark.execution.Sql2DataFrame
import com.adatao.ML.spark.recommendation.ALSUtils._

class CollaborativeFilteringSuite extends ABigRClientTest {
  test("Test ALS") {

    val xCols = Array(0, 1, 2)
    val rank = 8
    val iterations = 15
    val lambda = 8.0

    createTableRatings

    val cmd = new Sql2DataFrame("select * from ratings", true)
    val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](cmd).result
    assert(r0.isSuccess)
    val dataContainerID = r0.dataContainerID
    //val executor = new ALS(dataContainerID, xCols, rank, lambda, iterations)
    val executor = new ALS().setDataContainerID(dataContainerID).setTrainColumns(xCols).setNumFeatures(rank).setLambda(lambda).setNumIterations(iterations)
    val r = bigRClient.execute[IModel](executor)
    assert(r.isSuccess)

    val model = r.result.getRawModel.asInstanceOf[ALSModel]

    assert(model.predict(1, 3) > 0)
    assert(model.predict(2, Array(0, 1, 2, 3))(0) < 5)
    print("RMSE = " + model.getRmse())

  }

}