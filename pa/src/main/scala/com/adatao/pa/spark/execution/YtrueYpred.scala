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
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => DCModel}
import io.spark.ddf.content.RepresentationHandler
import org.apache.spark.mllib.linalg.Vectors
import io.ddf.content.Schema
import java.util
import scala.util
import io.ddf.content.Schema.Column

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
      case linearModel: ALinearModel[Double] => {
        LOG.info(">>> ALinearModel, running dummyCoding transformation")
        val dummyCodingDDF = ddf.getTransformationHandler.dummyCoding(xs, y(0))
        val rddMatrixVector = dummyCodingDDF.asInstanceOf[SparkDDF].getRDD(classOf[TupleMatrixVector])
        val ytrueYpredRDD = linearModel.yTrueYPred(rddMatrixVector)
        val schema = new Schema(null, "ytrue double, yPredict double")
        //val sparkDDF = new SparkDDF(ddf.getManager, ytrueYpredRDD, classOf[Array[Double]], ddf.getNamespace, null, schema)
        val sparkDDF: DDF = ddfManager.newDDF(ddf.getManager, ytrueYpredRDD, Array[Class[_]](classOf[RDD[_]], classOf[Array[Double]]), ddf.getNamespace, null, schema)
        sparkDDF.getName()
        sparkDDF
      }
      case decisionTree: DCModel => {
        LOG.info(">>>>> DecisionTreeModel, running prediction")
        val rddDouble = ddf.VIEWS.project(model.getTrainedColumns(): _*).getRepresentationHandler.
          get(RepresentationHandler.RDD_ARR_DOUBLE.getTypeSpecsString).asInstanceOf[RDD[Array[Double]]]
        val XsY = rddDouble.map{arr => (arr.take(arr.size - 1), arr(arr.size - 1))}
        val vectorLabel = XsY.map{case (arr, y) => (Vectors.dense(arr), y)}
        val rdd = vectorLabel.map{case (vec, y) => Array(y, decisionTree.predict(vec))}
        val outputColumns = new java.util.ArrayList[Schema.Column]()
        outputColumns.add(new Column("ytrue", "double"))
        outputColumns.add(new Column("yPredict", "double"))
        val schema: Schema = new Schema(null, outputColumns)
        /**
         * val resultDDF : DDF = this.getManager.newDDF(this.getManager, result.rdd, Array[Class[_]](classOf[Nothing],
         * classOf[Array[Double]]), this.getManager.getNamespace, null, schema)
         */
        ddfManager.newDDF(ddfManager, rdd, Array(classOf[RDD[_]], classOf[Array[Double]]), ddfManager.getNamespace, null, schema)
      }
      case _ => {
        val projectedDDF = ddf.VIEWS.project(model.getTrainedColumns: _*)
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

