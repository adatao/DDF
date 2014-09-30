package com.adatao.spark.ddf.analytics

import com.adatao.spark.ddf.ATestSuite
import com.adatao.spark.ddf.analytics.util.LabeledPoint

/**
 * author: daoduchuan
 */
class TFIDFSuite extends ATestSuite {

  test("test TFIDF") {
    val sparkContext = manager.getSparkContext
    val textRDD = sparkContext.textFile("/users/daoduchuan/ddf-enterprise/farm-data/farm-ads")
      .map{row => row.split(" ")}.map{row => row.toList}
    val rdd = textRDD.map{row => {
        val (label, data) = row.splitAt(1)
        val label2 = if(label(0).equals("1")) 1.0 else 0.0
        (data, label2)
      }
    }
    rdd.map{
      case(data, label) => data.mkString(";") + "," + label
    }.saveAsTextFile("/users/daoduchuan/ddf-enterprise/farm-data/featurize")
    val hashingTF = new HashingTF(1000)
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
    rddVecLabel.map{
      case (data, label) => (label, nbModel.predict(data))
    }.foreach {
      case (ytrue, ypred) => println(s"${ytrue}: ${ypred}")
    }
    //val tf_idf = idfModel.transform(rddVec).count

  }
}
