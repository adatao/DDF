package com.adatao.spark.ddf.analytics
import com.adatao.ddf.analytics.MLSupporter.Model
import org.apache.spark.mllib.regression.{LinearRegressionModel => MLlibLinearRegressionModel}
import java.util.{List => JList}
import com.adatao.ddf.DDF
import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.Schema
import com.adatao.spark.ddf.SparkDDF

/**
 * author: daoduchuan
 */
class LinearRegressionModel(mllibLinearModel: MLlibLinearRegressionModel, featureColumnNames: JList[String] = null) extends
  Model(featureColumnNames, classOf[Array[Double]]) {

  def this(mllibLinearModel: MLlibLinearRegressionModel) = this(mllibLinearModel, null)

  override  def isSupervisedAlgorithmModel(): Boolean = true

  override def predict(data: Object, ddf: DDF): DDF = {
    val rdd = data.asInstanceOf[RDD[Array[Double]]]
    val predRDD = mllibLinearModel.predict(rdd)

    val schema = new Schema(String.format("%s_%s", ddf.getTableName, "linearRegression_prediction"), "yPredict double")
    new SparkDDF(ddf.getManager, predRDD, classOf[Double], ddf.getManager.getNamespace, schema.getTableName, schema)
  }
}
