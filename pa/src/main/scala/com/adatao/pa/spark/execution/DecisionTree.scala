package com.adatao.pa.spark.execution

import java.util

import io.ddf.ml.{IModel, Model}
import io.ddf.{DDFManager, DDF}
import org.apache.spark.mllib.tree.configuration.{FeatureType, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Variance, Entropy}
import org.apache.spark.mllib.tree.model.{Split, Node}
import org.apache.spark.mllib.tree.{DecisionTree => SparkDT}
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

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

  //List of tuple (feature, operator, value)

  val rulesSet: java.util.List[String] = new util.ArrayList[String]()
  var rules = ""
  var indexRule = 1

  override def runImpl(ctx: ExecutionContext): DecisionTreeModel = {

    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
    val trainedColumns = xCols.map{idx => ddf.getColumnName(idx)} :+ ddf.getColumnName(yCol)
    val projectedDDF = ddf.VIEWS.project(trainedColumns: _*)

    val colFactors = xCols.map{colIx =>
      ddf.getColumn(ddf.getColumnName(colIx))}.filter{
      col => col.getOptionalFactor != null
    }
    val colFactorIdexes = colFactors.map{col => col.getName}.map{colName => projectedDDF.getColumnIndex(colName)}
    val factorMap = new GetMultiFactor(projectedDDF.getName, colFactorIdexes).run(ctx).result
    val mapCategorical = factorMap.map{case (idx, hmap) => (idx.toInt, hmap.size())}.toMap
    val maxBins = if(mapCategorical.size > 0) {
      val maxCategorical = mapCategorical.map{case (a,b)=> b}.max
      if(maxCategorical > 32) maxCategorical else 32
    } else {
      32
    }

    LOG.info(">>>> mapCategorical = " + mapCategorical.mkString(", "))
    val imp = impurity.toLowerCase() match {
      case "gini" => Gini
      case "variance" => Variance
      case "entropy" => Entropy
    }

    val strategy: Strategy = clazz.toLowerCase() match {
      case "classification" =>
        val numClasses = DecisionTree.getNumClasses(dataContainerID, yCol, ctx)
        new Strategy(algo = Classification, impurity = imp,
        maxDepth = maxDepth, numClassesForClassification = numClasses, maxBins = maxBins, categoricalFeaturesInfo= mapCategorical,
          minInstancesPerNode= minInstancePerNode, minInfoGain= minInfomationGain)

      case "regression" => new Strategy(algo = Regression, impurity = imp,
        maxDepth =maxDepth, numClassesForClassification = 10,  maxBins = maxBins, categoricalFeaturesInfo= mapCategorical,
        minInstancesPerNode= minInstancePerNode, minInfoGain= minInfomationGain)
    }
    val rddLabelPoint = projectedDDF.getRepresentationHandler.get(RepresentationHandler.RDD_LABELED_POINT.getTypeSpecsString).asInstanceOf[RDD[LabeledPoint]]
    val model = SparkDT.train(rddLabelPoint, strategy)

    LOG.info(">>>>> model " + model.toString())
    LOG.info(">>>>> model.topNode = " + model.topNode.toString())
    val imodel = new Model(model)
    imodel.setTrainedColumns(trainedColumns)
    manager.addModel(imodel)


    //read tree and generate rules
    //serialized built tree into instances set
    var root = model.topNode
    visitTree(root, "","",0.0)
    //print initial rule set

    println("\n")
    println(rules)

    val modelDescription = s"${model.toString}"
    val modelTree= serializedTree(model.topNode, 1)//.subtreeToString(1)
    new DecisionTreeModel(imodel.getName, modelDescription, modelTree, rules)
  }

  def splitToString(split: Split, left: Boolean): String = {
    split.featureType match {
      case FeatureType.Continuous => if (left) {
        s"(feature ${split.feature} <= ${split.threshold})"
      } else {
        s"(feature ${split.feature} > ${split.threshold})"
      }
      case FeatureType.Categorical => if (left) {
        s"(feature ${split.feature} in ${split.categories.mkString("{",",","}")})"
      } else {
        s"(feature ${split.feature} not in ${split.categories.mkString("{",",","}")})"
      }
    }
  }

  private def serializedTree(node: Node, indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    if (node.isLeaf) {
      prefix + s"Predict: ${node.predict.predict}\n"
    } else {
      prefix + s"If ${splitToString(node.split.get, left=true)}\n" +
        serializedTree(node.leftNode.get, indentFactor + 1) +
        prefix + s"Else ${splitToString(node.split.get, left=false)}\n" +
        serializedTree(node.rightNode.get, indentFactor + 1)
    }
  }

  def visitTree(node: Node, precedent: String, previousRule: String, previousThreshold: Double)  {
    if(!node.isLeaf) {
      //first concat current node to ruleset[length -1]
      //first get split
      var split = node.split.get
      if(split.featureType.equals(FeatureType.Continuous)) {
        var leftstr = "    feature " + split.feature + " <= " + split.threshold + "\n"

        if(previousRule.contains( "    feature " + split.feature) && previousThreshold > split.threshold) {
          //ignore previousRule
          var newprecedent = precedent.replace(previousRule, "")
          visitTree(node.leftNode.get, newprecedent + leftstr, leftstr, split.threshold)
        }
        else {
          visitTree(node.leftNode.get, precedent + leftstr ,leftstr, split.threshold)
        }


        var rightstr = "    feature " + split.feature + " > " + split.threshold + "\n"
        //adhoc optimization to remove non-sense rule
        if(previousRule.contains( "    feature " + split.feature) && previousThreshold < split.threshold) {
          //ignore previousRule
          var newprecedent = precedent.replace(previousRule, "")
          visitTree(node.rightNode.get, newprecedent + rightstr, rightstr, split.threshold)
        }
        else {
          visitTree(node.rightNode.get, precedent + rightstr, rightstr, split.threshold)
        }
      }
      else {
        var leftstr = "    feature " + split.feature + " in " + split.categories.toString() + "\n"
        var rightstr = "    feature " + split.feature + " not in " + split.categories.toString()+ "\n"
        visitTree(node.leftNode.get, precedent + leftstr, leftstr, split.threshold)
        visitTree(node.rightNode.get, precedent + rightstr, rightstr, split.threshold)
      }
    }
    else {
      //
      rules = rules + "\nRule " + indexRule +":\n" + precedent + "class no  [" + node.predict.predict.toString + "]" + "\n"
      indexRule = indexRule +  1

    }
    //rulesSet +=
    //var a = node.split.map(a => Seq(a.feature.toString, a.featureType.toString, a.threshold.toString))
    //rulesSet +: List(("1","2","3"))
  }
}

class DecisionTreeModel(val modelID: String, val description: String, val tree: String, val rules: String) extends Serializable

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
    //factors.map{ case (idx, hmap) => Map(idx.toInt, hmap.size())}
  }
}
