package com.adatao.spark.ddf.analytics.util

import com.adatao.spark.ddf.analytics.util.Vector

case class LabeledPoint(label: Double, features: Vector) {
  override def toString: String = {
    "(%s,%s)".format(label, features)
  }
}
