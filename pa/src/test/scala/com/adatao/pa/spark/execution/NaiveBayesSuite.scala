package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import io.ddf.ml.IModel

//import com.adatao.pa.spark.execution.NaiveBayes.NaiveBayesResult
import com.adatao.pa.spark.types.ATestBase
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult

class NaiveBayesSuite extends ABigRClientTest {


  test("test Naive Bayes classification") {
    val xCols = Array(1, 2, 3)
    val yCol = 0

    // train
    val dcIDtrain = this.loadFile(List("resources/admission.csv", "resources/admission.csv"), false, " ")
    var naiveBayesExe = new NaiveBayes(dcIDtrain, xCols, yCol, 0.9)
    var r = bigRClient.execute[IModel](naiveBayesExe)
    assert(r.isSuccess)
    var model = r.result


    // confusion matrix on train test
    var cfmExe = new BinaryConfusionMatrix(dcIDtrain, model.getName, 0.5)
    var cfm = bigRClient.execute[BinaryConfusionMatrixResult](cfmExe)
    assert(cfm.isSuccess)
    var cm = cfm.result

    assert(cm.truePos == 0)
    assert(cm.falsePos == 0)
    assert(cm.falseNeg == 127)
    assert(cm.trueNeg == 273)
  }
}
