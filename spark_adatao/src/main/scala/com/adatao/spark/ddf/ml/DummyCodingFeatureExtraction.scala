package com.adatao.spark.ddf.ml

import io.ddf.ml.FeatureExtraction
import io.ddf.DDF

/**
 */
class DummyCodingFeatureExtraction(columns: String*) extends FeatureExtraction {

  private var factorColumns: Seq[String] = Seq()

  def setFactorColumns(factorCols: String*) = {
    factorColumns = factorCols
  }

  override def extractFeature(ddf: DDF): DDF = {
    val projectedDDF = ddf.VIEWS.project(columns: _*)
    projectedDDF.getSchemaHandler.computeFactorLevelsForAllStringColumns()
    projectedDDF.getSchemaHandler.generateDummyCoding()
    projectedDDF
  }
}
