package com.adatao.pa.spark.execution

import io.basic.ddf.content.PersistenceHandler
import io.basic.ddf.BasicDDF
import io.ddf.ml.{IModel, Model}
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.adatao.spark.ddf.analytics.{NQLinearRegressionModel => NQLinearModel}
import com.adatao.pa.spark.types.{SuccessfulResult, FailedResult, ExecutionException, ExecutionResult}

/**
 * author: daoduchuan
 */
class LoadModel(uri: String) extends AExecutor[IModel] {
  override def runImpl(ctx: ExecutionContext): IModel = {
    val manager = ctx.sparkThread.getDDFManager
    val persistenceHandler = new PersistenceHandler(null)

    val modelDDF = persistenceHandler.load(uri).asInstanceOf[BasicDDF]
    val model = Model.deserializeFromDDF(modelDDF)
    manager.addModel(model)
    model
  }
}

