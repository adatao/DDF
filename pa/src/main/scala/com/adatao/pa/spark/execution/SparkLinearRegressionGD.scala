package com.adatao.pa.spark.execution

import io.ddf.ml.IModel

/**
 * author: daoduchuan
 */
class SparkLinearRegressionGD(dataContainerID: String, numIteration: Int, stepSize: Double)
  extends AExecutor[IModel] {

  override def runImpl(ctx: ExecutionContext): IModel = {
    val ddfManager = ctx.sparkThread.getDDFManager
    val ddf = ddfManager.getDDF(dataContainerID)
    ddf.ML.train("SparkLinearRegressionGD", numIteration: java.lang.Integer, stepSize: java.lang.Double)
  }
}
