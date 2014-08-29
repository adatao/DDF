package com.adatao.spark.ddf.ml

import io.ddf.ml.{StandardFeatureExtraction, FeatureExtraction}
import io.ddf.DDF
import scala.collection.JavaConversions._
import io.ddf.exception.DDFException
import io.ddf.content.Schema.DummyCoding

/**
 */
class DummyCodingFeatureExtraction(columns: String*) extends StandardFeatureExtraction {

  private var factorColumns: Seq[String] = Seq()

  private var featuresDDF: DDF = null;

  def setFactorColumns(factorCols: String*) = {
    factorColumns = factorCols
  }

  override def extractFeature(): DDF = {
    if(this.featuresDDF != null) {
      return featuresDDF;
    } else {
      featuresDDF = this.getDDF.VIEWS.project(columns: _*)
      featuresDDF.getSchemaHandler.setStringColumnsAsFactor()
      featuresDDF.getSchemaHandler.computeFactorLevelsAndLevelCounts()
      featuresDDF.getSchemaHandler.generateDummyCoding()
      featuresDDF
    }
  }

  def getNumberOfFeatures() = {
    if(this.featuresDDF == null) {
      this.extractFeature()
      if(this.featuresDDF == null) {
        throw new DDFException("this.featureDDF is null", null)
      }
    }
    this.featuresDDF.getSchema.getDummyCoding.getNumberFeatures
  }

  def getDummyCoding(): DummyCoding = {
    if(this.featuresDDF == null) {
      this.extractFeature()
      if(this.featuresDDF == null) {
        throw new DDFException("this.featureDDF is null", null)
      }
    }
    this.featuresDDF.getSchema.getDummyCoding
  }
}
