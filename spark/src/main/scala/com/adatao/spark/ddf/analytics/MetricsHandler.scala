package com.adatao.spark.ddf.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class MetricsHandler {

  object Metrics {
    
    val square = scala.math.pow(_: Double, 2)
    
    // Compute the R^2 score, aka http://en.wikipedia.org/wiki/Coefficient_of_determination
    
    def R2Score(yTrueYpred: RDD[(Double, Double)]): Double = {
      val meanYtrue = yTrueYpred.map({ case (ytrue, ypred) ⇒ ytrue }).mean()
      R2Score(yTrueYpred, meanYtrue)
    }

    // Compute the R^2 score given known yMean
    def R2Score(yTrueYpred: RDD[(Double, Double)], meanYtrue: Double): Double = {
      // collection of tuples of squared values of
      //   - difference from the mean
      //   - residuals
      val ss = yTrueYpred.map {
        case (ytrue, ypred) ⇒
          (square(ytrue - meanYtrue), square(ytrue - ypred))
      }

      // sum of squares
      val sssum = ss.reduce((a, b) ⇒ (a._1 + b._1, a._2 + b._2))
      val (sstot, ssres) = sssum
      if (sstot == 0) {
        1
      } else {
        1 - (ssres / sstot);
      }
    }
  }

}