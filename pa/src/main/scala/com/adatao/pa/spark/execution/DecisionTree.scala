package com.adatao.pa.spark.execution

import io.ddf.ml.IModel
import io.ddf.DDF
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Variance, Entropy}

/**
 * author: daoduchuan
 */
class DecisionTree(dataContainerID: String,
                   xCols: Array[Int],
                   yCol: Int,
                   clazz: String = Classification,
                   impurity: String = Gini,
                   maxDepth: Int = 10,
                   numClasses: Int = 10
                   ) extends AExecutor[IModel](true) {

  override def runImpl(ctx: ExecutionContext): IModel = {
    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
    val trainedColumns = xCols.map{idx => ddf.getColumnName(idx)} :+ ddf.getColumnName(yCol)
    clazz match {
      case "Classification" => new Strategy(algo = Classification, impurity = Gini,
        maxDepth = 10, numClassesForClassification = 10)
      case "Regression" => new Strategy(algo = Classification, impurity = Variance,
        maxDepth =10, numClassesForClassification = 10)
    }
    return null
  }
}
