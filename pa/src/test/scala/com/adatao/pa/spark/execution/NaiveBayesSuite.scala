package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.NaiveBayes.NaiveBayesResult
import com.adatao.pa.spark.types.ATestBase
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult

class NaiveBayesSuite extends ABigRClientTest {
  ignore("handle NA elements") {
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
			
    val naiveBayesExe = new NaiveBayes(dcIDtrain, xCols, yCol)
    val r = bigRClient.execute[NaiveBayesResult](naiveBayesExe)
    assert(r.isSuccess)
    val model = r.result
    assert(model.getNumClasses() == 2)
    assert(model.getNumFeatures() == 3)
  }

  ignore("test Naive Bayes classification") {
    val xCols = Array(1, 2, 3)
    val yCol = 0

    // train
    val dcIDtrain = this.loadFile(List("resources/admission.csv", "resources/admission.csv"), false, " ")
    var naiveBayesExe = new NaiveBayes(dcIDtrain, xCols, yCol)
    var r = bigRClient.execute[NaiveBayesResult](naiveBayesExe)
    assert(r.isSuccess)
    var model = r.result
    assert(model.getNumClasses() == 2)
    assert(model.getNumFeatures() == 3)

    // confusion matrix on train test
    var cfmExe = new BinaryConfusionMatrix(dcIDtrain, model.getModelID(), xCols, yCol, 0.5)
    var cfm = bigRClient.execute[BinaryConfusionMatrixResult](cfmExe)
    assert(cfm.isSuccess)
    var cm = cfm.result
    
    assert(cm.truePos == 26);
    assert(cm.falsePos == 17);
    assert(cm.falseNeg == 101);
    assert(cm.trueNeg == 256);
  }
}