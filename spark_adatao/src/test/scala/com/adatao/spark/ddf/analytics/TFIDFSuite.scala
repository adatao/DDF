package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.ATestSuite
import com.adatao.spark.ddf.analytics.util.LabeledPoint

/**
 * author: daoduchuan
 */
class TFIDFSuite extends ATestSuite {

  test("test TFIDF") {
    val sparkContext = manager.getSparkContext
    val textRDD = sparkContext.textFile("../resources/test/farm-ads")
      .map{row => row.split(" ")}.map{row => row.toList}
    val rdd = textRDD.map{row => {
        val (label, data) = row.splitAt(1)
        val label2 = if(label(0).equals("1")) 1.0 else 0.0
        (data, label2)
      }
    }

    val hashingTF = new HashingTF(100000)
    val rddVec = rdd.map {
      case (data, label) => hashingTF.transform(data)
    }

    val rddVecLabel = rdd.map{
      case (data, label) => (hashingTF.transform(data), label)
    }

    val idfModel: IDFModel = (new IDF()).fit(rddVec)
    val tf_idf = rddVecLabel.map {
      case (vector, label) => new LabeledPoint(label, idfModel.transform(vector))
    }
    val nb = new NaiveBayes()
    val nbModel = nb.run(tf_idf)
    //nbModel.predict(rddVec)
    val yTrueYPred = rddVecLabel.map{
      case (data, label) => (label, nbModel.predict(data))
    }
    val count: Double = yTrueYPred.count
    var correct: Double = 0.0
    yTrueYPred.collect().foreach {
      case (yTrue, yPred) => if(yTrue == yPred) correct += 1
    }
    println(s"correct: ${correct}, count: ${count}")
    println(">>> correct percentage = " + correct / count * 100 + "%")
    assert(correct === 3961.0)
    assert(count === 4143.0)
    //val tf_idf = idfModel.transform(rddVec).count
  }
}
