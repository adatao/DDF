package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import org.apache.spark.api.java.JavaRDD
import com.adatao.pa.spark.SharkUtils
import shark.api.JavaSharkContext
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.spark.ddf.SparkDDF

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/9/13
 * Time: 5:59 PM
 * To change this template use File | Settings | File Templates.
 */
class Residuals(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int)
  extends AExecutor[ResidualsResult] {
  override def runImpl(ctx: ExecutionContext): ResidualsResult = {

    val ddfManager = ctx.sparkThread.getDDFManager();
    val ddfId = dataContainerID
    val ddf: DDF = ddfManager.getDDF(ddfId);
    // first, compute RDD[(ytrue, ypred)]

    val mymodel: IModel = ddfManager.getModel(modelID)
    val predictionDDF = ddf.getMLSupporter().applyModel(mymodel, true, true)

    val residualsDDF = ddf.getMLMetricsSupporter().residuals(predictionDDF)
    require(residualsDDF != null)

    //return dataframe
    val metaInfo = Array(new MetaInfo("residual", "java.lang.Double"))
    val uid = residualsDDF.getName()

    new ResidualsResult(uid, metaInfo)
  }
}

class ResidualsResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
