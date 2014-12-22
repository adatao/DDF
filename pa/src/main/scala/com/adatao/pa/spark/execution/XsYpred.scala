package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.spark.ddf.analytics.Utils
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.ML.spark.clustering.KMeansModel
import com.adatao.spark.ddf.SparkDDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import io.ddf.content.Schema
import io.ddf.content.Schema.Column


/**
 * author: daoduchuan
 */
class XsYpred(dataContainerID: String, val modelID: String) extends AExecutor[DataFrameResult]{

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID)

    val model = manager.getModel(modelID)
    if(model == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "null model", null)
    }
//    val featureColumns = model.getTrainedColumns
//    val projectedDDF = ddf.VIEWS.project(featureColumns: _*)
//
//    val predictionDDF = projectedDDF.getMLSupporter.applyModel(model, false, true)
//    ddfManager.addDDF(predictionDDF)
    model.getRawModel match {
      case kmeansModel: KMeansModel => {
        val featureColumns = model.getTrainedColumns
        val projectedDDF = ddf.VIEWS.project(featureColumns: _*)
        val rddVector = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Vector])
        val predictionRDD = XsYpred.predict(kmeansModel, rddVector)
        val columns = featureColumns.map {
          str => new Column(str, Schema.ColumnType.DOUBLE)
        } :+ new Column("yPredict", Schema.ColumnType.DOUBLE)
        val schema = new Schema(null, columns)
        val predictionDDF = manager.newDDF(manager, predictionRDD, Array(classOf[RDD[_]], classOf[Vector]), manager.getNamespace, null, schema)
        manager.addDDF(predictionDDF)
        new DataFrameResult(predictionDDF)
      }
      case _ => {
        val featureColumns = model.getTrainedColumns
        val projectedDDF = ddf.VIEWS.project(featureColumns: _*)

        val predictionDDF = projectedDDF.getMLSupporter.applyModel(model, false, true)
        manager.addDDF(predictionDDF)
        new DataFrameResult(predictionDDF)
      }
    }
  }
}

object XsYpred {
  def predict(kmeansModel: KMeansModel, rdd: RDD[Vector]): RDD[Array[Double]] = {
    rdd.map{
      vector => vector.toArray :+ kmeansModel.predict(vector).toDouble
    }
  }
}
