package com.adatao.pa.spark.execution

import com.adatao.basic.ddf.content.PersistenceHandler
import com.adatao.basic.ddf.BasicDDF
import com.adatao.ddf.ml.Model
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
 * author: daoduchuan
 */
class LoadModel(uri: String) extends AExecutor[LoadModelResult] {
  override def runImpl(ctx: ExecutionContext): Object = {
    val manager = ctx.sparkThread.getDDFManager
    val persistenceHandler = new PersistenceHandler(null);
    val modelDDF = persistenceHandler.load(uri).asInstanceOf[BasicDDF]
    val model = Model.deserializeFromDDF(modelDDF);
    val rawModel = model.getRawModel
    val trainedCols = model.getTrainedColumns;
    rawModel match {
      case kmeansModel if kmeansModel.isInstanceOf[KMeansModel] => {

        new LoadModelResult(model.getTrainedColumns, rawModel, "Kmeans")
      }
      case nqModel if nqModel.isInstanceOf[NQLinearRegressionModel] => {
        new LoadModelResult(model.getTrainedColumns, rawModel, "NQLinearRegression")
      }
      case something => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error recognizing model: " +
        something.getClass.getName, null)
    }
  }
}

class LoadModelResult(val trainedColumns: Array[String], val model: Object, val modelType: String) extends Serializable
