package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.spark.ddf.analytics.Utils
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.ML.spark.clustering.KMeansModel
import com.adatao.ML.spark.recommendation.ALSModel
import com.adatao.spark.ddf.SparkDDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.DDF 


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
    val featureColumns = model.getTrainedColumns
    val projectedDDF = ddf.VIEWS.project(featureColumns: _*)
//
//    val predictionDDF = projectedDDF.getMLSupporter.applyModel(model, false, true)
//    ddfManager.addDDF(predictionDDF)
    model.getRawModel match {
      case kmeansModel: KMeansModel => {
//        val featureColumns = model.getTrainedColumns
//        val projectedDDF = ddf.VIEWS.project(featureColumns: _*)
        val rddVector = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Vector])
        val predictionRDD = XsYpred.predict(kmeansModel, rddVector)
        val columns = featureColumns.map {
          str => new Column(str, Schema.ColumnType.DOUBLE)
        } :+ new Column("yPredict", Schema.ColumnType.DOUBLE)
        val schema = new Schema(null, columns)
        val predictionDDF = manager.newDDF(manager, predictionRDD, Array(classOf[RDD[_]], classOf[Array[Double]]), manager.getNamespace, null, schema)
        manager.addDDF(predictionDDF)
        new DataFrameResult(predictionDDF)
      }
      case alsModel: ALSModel => {
        val rddUsersProducts = XsYpred.RDDRow2UserProduct(projectedDDF)
        val predictionRDD = alsModel.getMfModel.predict(rddUsersProducts)
        val columns = featureColumns.map {
          str => new Column(str, Schema.ColumnType.INT)
        } :+ new Column("predictedScore", Schema.ColumnType.DOUBLE)
        val schema = new Schema(null, columns)
        val predictionDDF = manager.newDDF(manager, predictionRDD, Array(classOf[RDD[_]], classOf[RDD[Rating]]), manager.getNamespace, null, schema)
        manager.addDDF(predictionDDF)
        new DataFrameResult(predictionDDF)
      }
      case _ => {
//        val featureColumns = model.getTrainedColumns
//        val projectedDDF = ddf.VIEWS.project(featureColumns: _*)

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
  
  def RDDRow2UserProduct(ddf: DDF): RDD[(Int, Int)] = {
    val data = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val rddUsersProducts = data.map(row ⇒ {
      val user = row.getInt(0)
      val product = row.getInt(1)
      (user, product)
    }).filter(row => row != null)
    rddUsersProducts
  }
}
