package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, MetaInfo}
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.{SharkUtils, DataManager}
import com.adatao.ML.{TModel, ALinearModel, TPredictiveModel}
import com.adatao.ML.types.Vector
import com.adatao.ML.spark.RddUtils

import org.apache.spark.api.java.JavaRDD
import shark.api.JavaSharkContext
import com.adatao.ddf.DDFManager

/**
 * Return predictions pair (ytrue, ypred) RDD in a DataFrame,
 * where ytrue and ypred are doubles.
 *
 * Works with LinearRegressionModel and LogisticRegressionModel.
 *
 * @author aht
 */
class YtrueYpred(dataContainerID: String, val modelID: String) extends AExecutor[YtrueYpredResult] {
	override def runImpl(ctx: ExecutionContext): YtrueYpredResult = {

    val ddfManager = ctx.sparkThread.getDDFManager;
    val ddf = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"))
    ddf.ML.applyModel();
	}
}

class YtrueYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
