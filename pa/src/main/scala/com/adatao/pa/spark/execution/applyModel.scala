package com.adatao.pa.spark.execution

/**
 * author: daoduchuan
 */
class applyModel(dataContainerID: String, val modelID: String, val hasLabels: Boolean, val includeFeatures: Boolean) extends AExecutor[applyModelResult] {
  override def runImpl(ctx: ExecutionContext): applyModelResult = {

    val ddfManager = ctx.sparkThread.getDDFManager;
    val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"))
    val model = ddfManager.getModel(modelID);
    val newDDF = ddf.ML.applyModel(model, hasLabels, includeFeatures);
    return new applyModelResult(newDDF.getName());
  }
}

class applyModelResult(val dataContainerID: String)
