package com.adatao.spark.ddf.etl

import io.ddf.DDF
import io.spark.ddf.etl.{TransformationHandler => THandler}

/**
 */
class TransformationHandler(mDDF: DDF) extends THandler(mDDF) {

  def dummyCoding(xCols: Array[String], yCol: String): DDF = {
    //TODO @Khang please implement the optimized dummyCoding version here
    //the return DDF will contain single representation which is TupleMatrixVector
    val trainedColumns = (xCols :+ yCol)
    mDDF.VIEWS.project(trainedColumns: _*)
  }
}
