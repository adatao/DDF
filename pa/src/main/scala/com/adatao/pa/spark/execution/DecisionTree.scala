package com.adatao.pa.spark.execution

import io.ddf.ml.IModel
import io.ddf.DDF
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Variance, Entropy}
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * author: daoduchuan
 */
class DecisionTree(dataContainerID: String,
                   xCols: Array[Int],
                   yCol: Int,
                   clazz: String = "Classification",
                   impurity: String = "Gini",
                   maxDepth: Int = 10
                   ) extends AExecutor[IModel](true) {

  override def runImpl(ctx: ExecutionContext): IModel = {

    val manager = ctx.sparkThread.getDDFManager
    val projectedDDF = manager.getDDF(dataContainerID) match {
      case ddf: DDF => {
        val trainedColumns = xCols.map{idx => ddf.getColumnName(idx)} :+ ddf.getColumnName(yCol)
        ddf.VIEWS.project(trainedColumns: _*)
      }
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }

    val imp = impurity.toLowerCase() match {
      case "gini" => Gini
      case "variance" => Variance
      case "entropy" => Entropy
    }
    val numClasses = DecisionTree.getNumClasses(dataContainerID, yCol, ctx)
    val strategy = clazz.toLowerCase() match {
      case "classification" => new Strategy(algo = Classification, impurity = imp,
        maxDepth = 10, numClassesForClassification = numClasses)
      case "regression" => new Strategy(algo = Classification, impurity = imp,
        maxDepth =10, numClassesForClassification = numClasses)
    }

    //TODO, run as.factor to getNumClasses
    projectedDDF.ML.train("decisionTree", strategy)
  }
}

object DecisionTree {
  def getNumClasses(dataContainerID: String, colIndex: Int, ctx: ExecutionContext): Int = {
    val ddf = ctx.sparkThread.getDDFManager.getDDF(dataContainerID)
    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF dataContainerId= " + dataContainerID + "\t ddfId = " + dataContainerID, null)
    }
    val exec = new GetMultiFactor(dataContainerID, Array(colIndex))
    val a = exec.run(ctx).result
    val b = a.filter {
      case (idx, hmap) => idx == colIndex
    }
    a(0)._2.size()
  }
}
