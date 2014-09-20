package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, MetaInfo}
import com.adatao.spark.ddf.analytics.TModel
import io.ddf.types.TupleMatrixVector
import com.adatao.spark.ddf.analytics._
import org.apache.spark.api.java.JavaRDD
import com.adatao.spark.ddf.analytics._
import io.ddf.DDF
import io.ddf.ml.IModel
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.Utils._
import com.adatao.spark.ddf.etl.TransformationHandler._
import io.ddf.content.Schema
import io.spark.ddf.SparkDDF

/**
 * Return predictions pair (ytrue, ypred) RDD in a DataFrame,
 * where ytrue and ypred are doubles.
 *
 * Works with LinearRegressionModel and LogisticRegressionModel.
 *
 */
class YtrueYpred(dataContainerID: String, val modelID: String) extends AExecutor[DataFrameResult] {
  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val ddfManager = ctx.sparkThread.getDDFManager()
    val ddf: DDF = ddfManager.getDDF(dataContainerID)

    //apply model on dataContainerID
    val model: IModel = ddfManager.getModel(modelID)
    val trainedColumns = model.getTrainedColumns
    val (xs, y) = trainedColumns.splitAt(trainedColumns.size - 1)

    val predictionDDF = model.getRawModel match {
      case linearModel: AContinuousIterativeLinearModel => {
        LOG.info(">>> ALinearModel, running dummyCoding transformation")
        val dummyCodingDDF = ddf.getTransformationHandler.dummyCoding(xs, y(0))
        val rddMatrixVector = dummyCodingDDF.getRDD(classOf[TupleMatrixVector])
        val ytrueYpredRDD = linearModel.yTrueYPred(rddMatrixVector)
        val schema = new Schema(null, "ytrue double, yPredict double")
        
        
        val testdata = ytrueYpredRDD.collect()
        println(">>>>>>>>>>>>>>>>>>>> ytrueypred running linearModel: ytrueypreidct data = " + testdata(0)(0) + "," +  testdata(0)(1))
        
        new SparkDDF(ddf.getManager, ytrueYpredRDD, classOf[Array[Double]], ddf.getNamespace, null, schema)
      }
      case _ => {
        val projectedDDF = ddf.VIEWS.project(model.getTrainedColumns: _*)
        
        println(">>>>>>>>>>>>>>>>>>> ytrueypred not linearModel ")
        
        projectedDDF.getMLSupporter().applyModel(model, true, false)
      }
    }

    if (predictionDDF == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error predicting, prediction DDF is null.", null)
    } else {
      val predID = ddfManager.addDDF(predictionDDF)
      LOG.info(">>>>> prediction DDF = " + predID)
    }
    new DataFrameResult(predictionDDF)
  }
}

