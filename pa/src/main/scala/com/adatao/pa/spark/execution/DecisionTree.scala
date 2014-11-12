package com.adatao.pa.spark.execution

import io.ddf.ml.{IModel, Model}
import io.ddf.{DDFManager, DDF}
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Variance, Entropy}
import org.apache.spark.mllib.tree.{DecisionTree => SparkDT}
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * author: daoduchuan
 */
class DecisionTree(dataContainerID: String,
                   xCols: Array[Int],
                   yCol: Int,
                   clazz: String = "Classification",
                   impurity: String = "Gini",
                   maxDepth: Int = 10,
                   minInstancePerNode: Int = 1,
                   minInfomationGain: Double = 0.0
                   ) extends AExecutor[DecisionTreeModel](true) {

  override def runImpl(ctx: ExecutionContext): DecisionTreeModel = {

    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
    val trainedColumns = xCols.map{idx => ddf.getColumnName(idx)} :+ ddf.getColumnName(yCol)
    val projectedDDF = ddf.VIEWS.project(trainedColumns: _*)


    val imp = impurity.toLowerCase() match {
      case "gini" => Gini
      case "variance" => Variance
      case "entropy" => Entropy
    }

    val strategy: Strategy = clazz.toLowerCase() match {
      case "classification" =>
        val numClasses = DecisionTree.getNumClasses(dataContainerID, yCol, ctx)
        new Strategy(algo = Classification, impurity = imp,
        maxDepth = maxDepth, numClassesForClassification = numClasses,
          minInstancesPerNode= minInstancePerNode, minInfoGain= minInfomationGain)

      case "regression" => new Strategy(algo = Classification, impurity = imp,
        maxDepth =maxDepth, numClassesForClassification = 10,
        minInstancesPerNode= minInstancePerNode, minInfoGain= minInfomationGain)
    }
    val rddLabelPoint = projectedDDF.getRepresentationHandler.get(RepresentationHandler.RDD_LABELED_POINT.getTypeSpecsString).asInstanceOf[RDD[LabeledPoint]]

    val model = SparkDT.train(rddLabelPoint, strategy)

    LOG.info(">>>>> model " + model.toString())
    LOG.info(">>>>> model.topNode = " +model.topNode.toString())
    val imodel = new Model(model)
    imodel.setTrainedColumns(trainedColumns)
    manager.addModel(imodel)
    val modelDescription = s"${model.toString} \n ${model.topNode.toString()}"
    val modelTree= model.topNode.subtreeToString(1)
    new DecisionTreeModel(imodel.getName, modelDescription, modelTree)
  }
}

class DecisionTreeModel(val modelID: String, val description: String, val tree: String) extends Serializable

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
