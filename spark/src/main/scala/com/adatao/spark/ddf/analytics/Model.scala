package com.adatao.spark.ddf.analytics

import com.adatao.ddf.DDF
import com.adatao.ddf.analytics.ISupportML.IModelParameters

/**
 * author: daoduchuan
 */


class Model(predictionInputClass: Class[_], params: IModelParameters, featureColumnIndexes: Array[Int])
    extends com.adatao.ddf.analytics.MLSupporter.Model(predictionInputClass, params, featureColumnIndexes){

  override def prepareData(ddf: DDF): Object = {
    ddf.getRepresentationHandler.get(this.getPredictionInputClass);
  }

  override def predict(ddf: DDF): DDF = {

  }
}
