package com.adatao.spark.ddf.ml
import io.ddf.facades.{MLFacade => IOMLFacade}
import io.ddf.DDF
import io.ddf.ml.{IModel, ISupportML}
import com.adatao.spark.ddf.etl.TransformationHandler

/**
 */
class MLFacade(theDDF: DDF, mlSupporter: ISupportML) extends IOMLFacade(theDDF, mlSupporter) {

  //TODO @Khang  DummyCoding here, numFeatures should not required for input parameters
  //also return model must incoporate dummyCoding information
  def LinearRegressionNQ(xCols: Array[String], yCol: String, ridgeLambda: Double): IModel = {
    val transformedDDF = theDDF.getTransformationHandler.asInstanceOf[TransformationHandler].dummyCoding(xCols, yCol)
    transformedDDF.getSchemaHandler.computeFactorLevelsForAllStringColumns()

    transformedDDF.getSchemaHandler.generateDummyCoding()
    val numFeatures: Int = if(transformedDDF.getSchema.getDummyCoding != null) {
      transformedDDF.getSchema.getDummyCoding.getNumberFeatures
    } else {
      xCols.length + 1
    }

    val model = transformedDDF.ML.train("linearRegressionNQ", numFeatures: java.lang.Integer, ridgeLambda: java.lang.Double)
    val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.NQLinearRegressionModel]
    if(transformedDDF.getSchema.getDummyCoding != null) {
      rawModel.setDummy(transformedDDF.getSchema.getDummyCoding)
    }

    model
  }
}
