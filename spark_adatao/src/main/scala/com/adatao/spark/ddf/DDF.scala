package com.adatao.spark.ddf

import io.spark.ddf.SparkDDF
import io.ddf.DDFManager
import io.ddf.content.Schema
import com.adatao.spark.ddf.ml.MLFacade

/**
 */
class DDF(manager: DDFManager, data: Object, typeSpecs: Array[Class[_]], namespace: String, name: String,
          schema: Schema) extends SparkDDF(manager, data, typeSpecs, namespace, name, schema) {

  this.ML = new MLFacade(this, this.getMLSupporter)

  def this(manager: DDFManager) = {
    this(manager, null, null, manager.getNamespace, null, null)
  }
}
