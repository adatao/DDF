package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, MetaInfo}
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.{SharkUtils, DataManager}
import com.adatao.ML.{TModel, ALinearModel, TPredictiveModel}
import com.adatao.ML.types.Vector
import com.adatao.ML.spark.RddUtils

import org.apache.spark.api.java.JavaRDD
import shark.api.JavaSharkContext

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
		val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)

		// box into Objects, so to surface it as a DataFrame for the user to examine from client
		val boxedPredictions = predictions.map { case (ytrue, ypred) =>
			Array(ytrue.asInstanceOf[Object], ypred.asInstanceOf[Object])
		}

		// synthesize the MetaInfo
		val metaInfo = Array(new MetaInfo("ytrue", "java.lang.Double"), new MetaInfo("ypred", "java.lang.Double"))

		// add to DataManager
		val df = new DataFrame(metaInfo, JavaRDD.fromRDD(boxedPredictions))
		val jsc =   ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
		val sdf= SharkUtils.createSharkDataFrame(df, jsc)
		val uid = ctx.sparkThread.getDataManager.add(sdf)

		new YtrueYpredResult(uid, metaInfo)
	}
}

class YtrueYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
