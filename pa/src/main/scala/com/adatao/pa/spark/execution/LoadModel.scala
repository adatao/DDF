package com.adatao.pa.spark.execution

import com.adatao.basic.ddf.content.PersistenceHandler
import com.adatao.basic.ddf.BasicDDF
import com.adatao.ddf.ml.Model
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.adatao.pa.spark.types.{SuccessfulResult, FailedResult, ExecutionException, ExecutionResult}

/**
 * author: daoduchuan
 */
class LoadModel(uri: String) extends AExecutor[LoadModelResult] {
  override def runImpl(ctx: ExecutionContext): LoadModelResult = {
    val manager = ctx.sparkThread.getDDFManager
    val persistenceHandler = new PersistenceHandler(null);
    val modelDDF = persistenceHandler.load(uri).asInstanceOf[BasicDDF]
    val model = Model.deserializeFromDDF(modelDDF);
    val rawModel = model.getRawModel
    val trainedCols = model.getTrainedColumns;
    rawModel match {
      case kmeansModel if kmeansModel.isInstanceOf[KMeansModel] => {
        //LOG.info(">>>>>> model.getTrainedColumns = " + model.getTrainedColumns.mkString(", "))
        new LoadModelResult(model.getName, model.getTrainedColumns, rawModel, rawModel.getClass.toString)
      }
      case nqModel if nqModel.isInstanceOf[NQLinearRegressionModel] => {
        new LoadModelResult(model.getName, model.getTrainedColumns, rawModel, "NQLinearRegression")
      }
      case something => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error recognizing model: " +
        something.getClass.getName, null)
    }
  }

  override def run(context: ExecutionContext): ExecutionResult[LoadModelResult] = {
    try {
      val result = new SuccessfulResult(this.runImpl(context))
      result.persistenceID = result.result.modelID
      result
    } catch {
      case e: ExecutionException => new FailedResult[LoadModelResult](e.message)
    }
  }
}

class LoadModelResult(val modelID: String, val trainedColumns: Array[String], val model: Object, val modelType: String) extends Serializable
