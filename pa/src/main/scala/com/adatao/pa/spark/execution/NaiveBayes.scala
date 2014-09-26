package com.adatao.pa.spark.execution

import io.ddf.DDF
import io.spark.ddf.content.RepresentationHandler
import io.ddf.ml.IModel
import io.ddf.ml.Model
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 */
class NaiveBayes(dataContainerID: String,
                 xCols: Array[Int],
                 yCol: Int,
                 alpha: Double) extends AExecutor[IModel] {

  override def runImpl(ctx: ExecutionContext): IModel = {
    val ddfManager = ctx.sparkThread.getDDFManager
    val ddf = ddfManager.getDDF(dataContainerID) match {
      case x: DDF => x
      case _ => throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Can not find DDF " + dataContainerID, null)
    }
    val trainedColumns = (xCols :+ yCol).map(idx => ddf.getColumnName(idx))
    val projectedDDF = ddf.VIEWS.project(trainedColumns: _*)
    projectedDDF.ML.train("NaiveBayes", alpha: java.lang.Double)
  }
}
