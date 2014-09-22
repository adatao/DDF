package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import org.apache.spark.api.java.JavaRDD
import io.ddf.DDF
import io.ddf.ml.IModel
import io.spark.ddf.SparkDDF
import com.adatao.pa.spark.Utils._
/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/9/13
 * Time: 5:59 PM
 * To change this template use File | Settings | File Templates.
 */
class Residuals(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int)
  extends AExecutor[DataFrameResult] {
  override def runImpl(ctx: ExecutionContext): DataFrameResult = {

    val ddfManager = ctx.sparkThread.getDDFManager()

    val yTrueYpred = new YtrueYpred(dataContainerID, modelID).runImpl(ctx)

    val predictionDDF = ddfManager.getDDF(yTrueYpred.dataContainerID)
    val residualsDDF = predictionDDF.getMLMetricsSupporter().residuals()
    require(residualsDDF != null)
    new DataFrameResult(residualsDDF)
  }
}

