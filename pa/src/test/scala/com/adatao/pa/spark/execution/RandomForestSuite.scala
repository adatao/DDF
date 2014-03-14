package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.ML.RandomForestModel
import com.adatao.pa.spark.execution.RandomForest.RandomForestResult
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult

class RandomForestSuite extends ABigRClientTest {
  test("handle NA elements") {
    val xCols = Array(1, 2, 3)
    val yCol = 0
    val mTry = 0
    val numTrees = 20
    val seed = 1988

    val dcIDtrain = bigRClient.execute[LoadTable.LoadTableResult](
			new LoadTable()
			.setFileURL("resources/admissionWithNa.csv")
			.setHasHeader(false)
			.setSeparator(" ")
			.setSampleSize(5)
			.setDoPreferDouble(false)
			).result.getDataContainerID

    val randomForestExe = new RandomForest(dcIDtrain, xCols, yCol, mTry, numTrees, seed)
    val r = bigRClient.execute[RandomForestResult](randomForestExe)
    assert(r.isSuccess)
    var model = r.result
    assert(model.seed == seed)
    assert(model.mtry == 2)  // classification => ceil (sqrt(4-1)) = ceil(1.73) = 2
    assert(model.numTrees == numTrees)
  }

  test("test Random Forest regression") {
    val xCols = Array(0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    val yCol = 7
    val mTry = 0
    val numTrees = 11
    val seed = 1954

    val dcIDtrain = this.loadFile(List("resources/airline-transform.3.csv", "server/resources/airline-transform.3.csv"), false, ",")

    // train
    var randomForestExe = new RandomForest(dcIDtrain, xCols, yCol, mTry, numTrees, seed)
    var r = bigRClient.execute[RandomForestResult](randomForestExe)
    assert(r.isSuccess)
    var model = r.result
    assert(model.seed == seed)
    assert(model.mtry == 6)  // regression => ceil ((17-1) / 3) = 6
    assert(model.numTrees == numTrees)
            
    // evaluation
    var r2scorer = new R2Score(dcIDtrain, xCols, yCol, model.modelID)
    var r2 = bigRClient.execute[Double](r2scorer)
    assert(r2.isSuccess)
    LOG.info("R2 Score = " + r2.result)
    
    // assertion
    assert(Math.abs(0.9790220492182647 - r2.result) < 1E-5)
  }

  test("test Random Forest classification") {
    val xCols = Array(1, 2, 3)
    val yCol = 0
    val mTry = 0
    val numTrees = 30
    val seed = 1945

    createTableAdmission
    val df = this.runSQL2RDDCmd("SELECT * FROM admission", true)
    assert(df.isSuccess)
    val dcIDtrain = df.dataContainerID
    LOG.info("Got dataContainerID= " + dcIDtrain)

    // train
    val randomForestExe = new RandomForest(dcIDtrain, xCols, yCol, mTry, numTrees, seed)
    val r = bigRClient.execute[RandomForestResult](randomForestExe)
    assert(r.isSuccess)
    val res = r.result
    assert(res.seed == seed)
    assert(res.numTrees == numTrees)
    assert(res.mtry == 2)  // classification => ceil (sqrt(4-1)) = ceil(1.73) = 2

    if (res.cfm != null) {
        assert(res.cfm(0) == 39)
        assert(res.cfm(1) == 37)
        assert(res.cfm(2) == 88)
        assert(res.cfm(3) == 236)
        assert(res.cfm(0) + res.cfm(1) + res.cfm(2) + res.cfm(3) == 400)
    }
  }
}