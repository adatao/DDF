package com.adatao.pa.spark.DDF

import com.adatao.ddf.ml.IModel
import com.adatao.pa.spark.execution.PersistModel
import com.adatao.pa.spark.DDF.DDFManager.client


/**
 * author: daoduchuan
 */
class ObjectDDF(var name: String= null, model: IModel) {
  this.name = model.getName
  def persist(alias: String = null): String = {
    val cmd = new PersistModel(model.getName, alias)
    client.execute[String](cmd).result
  }
}
