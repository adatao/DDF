package com.adatao.pa.spark.DDF

import com.adatao.ddf.ml.IModel
import com.adatao.pa.spark.execution.PersistModel
import com.adatao.pa.ddf.spark.DDFManager
import com.adatao.pa.ddf.spark.DDFManager.client


/**
 * author: daoduchuan
 */
class ObjectDDF(model: IModel) {

  val name = model.getName
  def persist(): String = {
    val cmd = new PersistModel(model.getName)
    client.execute[String](cmd).result
  }
}
