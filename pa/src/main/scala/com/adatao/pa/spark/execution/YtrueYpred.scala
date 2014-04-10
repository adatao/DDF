package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.{ SharkUtils, DataManager }
import com.adatao.ML.{ TModel, ALinearModel, TPredictiveModel }
import com.adatao.ddf.types.Vector
import com.adatao.ML.spark.RddUtils
import org.apache.spark.api.java.JavaRDD
import shark.api.JavaSharkContext
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * Return predictions pair (ytrue, ypred) RDD in a DataFrame,
 * where ytrue and ypred are doubles.
 *
 * Works with LinearRegressionModel and LogisticRegressionModel.
 *
 * @author aht
 */
class YtrueYpred(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int) extends AExecutor[YtrueYpredResult] {
  override def runImpl(ctx: ExecutionContext): YtrueYpredResult = {

    // first, compute RDD[(ytrue, ypred)]
    //		val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)
    //
    //		// box into Objects, so to surface it as a DataFrame for the user to examine from client
    //		val boxedPredictions = predictions.map { case (ytrue, ypred) =>
    //			Array(ytrue.asInstanceOf[Object], ypred.asInstanceOf[Object])
    //		}
    //
    //		// synthesize the MetaInfo
    //		val metaInfo = Array(new MetaInfo("ytrue", "java.lang.Double"), new MetaInfo("ypred", "java.lang.Double"))
    //
    //		// add to DataManager
    //		val df = new DataFrame(metaInfo, JavaRDD.fromRDD(boxedPredictions))
    //		val jsc =   ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
    //		val sdf= SharkUtils.createSharkDataFrame(df, jsc)
    //		val uid = ctx.sparkThread.getDataManager.add(sdf)
    //		
    //
    //		new YtrueYpredResult(uid, metaInfo)

    val ddfManager = ctx.sparkThread.getDDFManager();
    val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
    // first, compute RDD[(ytrue, ypred)]

    //apply model on dataContainerID
    val mymodel: IModel = ddfManager.getModel(modelID)
    val predictionDDF = ddf.getMLSupporter().applyModel(mymodel, true, true)

    if(predictionDDF != null) ddf.getManager().addDDF(predictionDDF);
    
    //return DDF
    val metaInfo = Array(new MetaInfo("ytrue", "java.lang.Double"), new MetaInfo("yPredict", "java.lang.Double"))
    val uid = predictionDDF.getName().replace("_", "-").replace("SparkDDF-spark-", "").replace("-com.adatao.ML.LogisticRegressionModel-YTrueYPredict", "")

    new YtrueYpredResult(uid, metaInfo)
  }
}

class YtrueYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
