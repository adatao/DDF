/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 *
 */
package com.adatao.pa.spark.execution
import scala.Array.canBuildFrom
import com.google.gson.Gson
import com.adatao.ML.types.Matrix
import com.adatao.ML._
import com.adatao.ML.types.Vector
import com.adatao.pa.spark.SparkThread
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.DataManager

import com.adatao.pa.spark.DataManager.{SharkDataFrame, DataFrame}
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessfulResult
import com.adatao.pa.spark.types.FailedResult

import com.adatao.ML.Kmeans.ParsePoint
import scala.Some
import com.adatao.pa.spark.types.ExecutionException
import scala.reflect.Manifest
import scala.collection.JavaConversions._
import com.adatao.ML.spark.Predictions
import com.adatao.ML.spark.TransformDummyCoding
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import java.util.HashMap
import org.apache.spark.api.java.JavaSparkContext

/**
 * These classes belong to the package [[com.adatao.pa.spark.execution]], which concern
 * themselves with how to execute algorithms within the BigR Spark environment.
 * "Pure" algorithms belong to the package [[com.adatao.spark.ML]]. In particular, the
 * class hierarchy and interfaces are defined such that they are not bothered with
 * the requirements of the execution environment.
 *
 * There is an [[com.adatao.pa.spark.execution.LinearRegression]] corresponding to
 * an [[com.adatao.spark.ML.LinearRegression]]. The [[execution.LinearRegression]] is the abstract [[AExecutor]]
 * interface that will be called by the Spark execution environment, via the
 * method `run()`. It also handles any data processing that is needed to map data from
 * the execution environment to the format expected by the pure algorithm.
 *
 * [[execution.LinearRegression]] in turn instantiates and invokes a "pure-algorithm"
 * [[ML.LinearRegression]] object. [[ML.LinearRegression]] returns an [[ML.LinearRegressionPredictor]],
 * which is then packaged within a [[SuccessfulResult]] by [[execution.LinearRegression]],
 * and the whole thing is returned to the (Thrift) client.
 *
 * @author ctn
 */

/**
 * Base implementation of TExecutor
 *
 * @author ctn
 *
 */
abstract class AExecutor[ResultType](var doPersistResult: Boolean = false)(implicit _rtm: Manifest[ResultType]) extends TExecutor[ResultType] with TCanLog with Serializable {

	/**
	 * This typically is applicable across most executors and don't need to be overridden.
	 * Try overriding [[runImpl]] instead.
	 */
	def run(context: ExecutionContext): ExecutionResult[ResultType] = {
		try {
			val result = new SuccessfulResult(this.runImpl(context))
			if (doPersistResult) result.persistenceID = context.sparkThread.getDataManager.putObject(result.result)
			result
		}
		catch {
			case ee: ExecutionException ⇒ new FailedResult[ResultType](ee.message)
		}
	}

	/**
	 * Override [[runImpl]] to do your own run() work
	 */
	protected def runImpl(context: ExecutionContext): ResultType

	/**
	 * Gets an RDD[Array[Object]] from dataContainerID in context
	 */
	def getRDD(dataContainerID: String, context: ExecutionContext): Option[RDD[Array[Object]]] = {
		Option(context.sparkThread.getDataManager.get(dataContainerID)).map(df ⇒ df.getRDD.rdd)
	}

	/*
	 * Get (ytrue, ypred) predictions RDD of Double
	 */
	def getYtrueYpred(
		dataContainerID: String,
		modelID: String,
		xCols: Array[Int],
		yCol: Int,
		context: ExecutionContext): RDD[(Double, Double)] = {
		val dm = context.sparkThread.getDataManager

		val dataContainer = dm.get(dataContainerID)
		if (dataContainer == null) throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"dataContainerID %s doesn't exist in user session".format(dataContainerID), null)

		val model = dm.getObject(modelID).asInstanceOf[TModel]
		if (model == null) throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"modelID %s doesn't exist in user session".format(modelID), null)

		Predictions.yTrueYpred(model, dataContainer, xCols, yCol)
		
//		if (dataContainer.getType == ContainerType.SharkDataFrame) {
			// this execution path is fastest when data matrix was already cached,
			// which is typical because you'd predict on a trained model, hence cached table,
			// but on non-cached data, such as cross-validation it will regenerate
			// a throw-away RDD[(Matrix, Vector)]
//			val dataframe = dataContainer.asInstanceOf[DataManager.SharkDataFrame]
//			Predictions.yTrueYpred(model, dataContainer, xCols, yCol)
//		}
//		else {
//			// normal path that goes thru DataFrame and object boxing
//			Predictions.yTrueYpred(model, dataContainer, xCols, yCol)
//		}
	}

	def getXsYpred(
			dataContainerID: String,
			modelID: String,
			xCols: Array[Int],
			context: ExecutionContext): RDD[(Array[Double],Int)] = {
		val dm = context.sparkThread.getDataManager
		val dataContainer = dm.get(dataContainerID)
		if(dataContainer == null) throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"dataContainerID %s doesn't exist in user session".format(dataContainerID), null)

		val model = dm.getObject(modelID).asInstanceOf[TModel]
		if (model == null) throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"modelID %s doesn't exist in user session".format(modelID), null)

		dataContainer match{
			case sdf: SharkDataFrame => Predictions.XsYpred(model, sdf, xCols)
			case df: DataFrame => Predictions.XsYpred(model, df.getRDD.rdd, xCols)
		}
	}

}

/**
 * Trait (interface) for an Executor (e.g., algorithm) to be invoked by the Spark runner.
 *
 * @author ctn
 *
 */
trait TExecutor[ResultType] {
	def run(context: ExecutionContext): ExecutionResult[ResultType]

	/**
	 * A basic toString implementation that simply dumps out all the field names and values
	 */
	override def toString: String = {
		"%s [%s]".format(
			TExecutor.this.getClass.getSimpleName, ((TExecutor.this.getClass.getFields).map(
				f ⇒ "%s=%s".format(f.getName, new Gson().toJson(f.get(TExecutor.this)))
			)).mkString(", ")
		)
	}
}

/**
 * Context info to be passed into to TExecutor.run(), e.g., SparkThread, etc.
 */
class ExecutionContext(val sparkThread: SparkThread) {
	// val hadoopConfig = sparkThread.getSparkContext().hadoopConfiguration
	val hadoopConfig = null
	// val sparkContext = sparkThread.getSparkContext()
	val sparkContext: JavaSparkContext = null
}

abstract class AModelTrainer[T <: TModel](
		val dataContainerID: String,
		val xCols: Array[Int],
		val yCol: Int,
		val mapReferenceLevel: HashMap[String, String] = null)(implicit m: Manifest[T]) extends AExecutor[T](doPersistResult = true) {

	var dummyColumnMapping = new HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]()
	var numFeatures: Int = 0
	override def runImpl(ctx: ExecutionContext) = train(dataContainerID, ctx)

	/**
	 * Gets an Option(RDD[[(Matrix, Vector)]]) from dataContainerID in context
	 */
	def getDataPartition(dataContainerID: String, xCols: Array[Int], yCol: Int, context: ExecutionContext): Option[RDD[(Matrix, Vector)]] = {

		//update numFeatures
		numFeatures = xCols.length + 1
		//handle both shark dataframe and normal dataframe
		Option(context.sparkThread.getDataManager.get(dataContainerID)) match {
			case Some(dataContainer) ⇒ {

				var trDummyCoding = new TransformDummyCoding(dataContainer.getMetaInfo(), xCols, mapReferenceLevel)

				var (dataPartition, dcm) = trDummyCoding.transform(dataContainer, yCol)
				//set property
				LOG.info("dummyColumnMapping={}",dcm.toString())
				dummyColumnMapping = dcm
				if (dummyColumnMapping.size() > 0)
					numFeatures += trDummyCoding.numDummyCols
				Option(dataPartition)

			}
			case None ⇒ throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"dataContainerID %s doesn't exist in user session".format(dataContainerID), null)
		}
	}

	/*
	 * 
	 */
	def train(dataContainerID: String, context: ExecutionContext): T = {
		this.getDataPartition(dataContainerID, xCols, yCol, context) match {
			case Some(dataPartition) ⇒ {
				dataPartition.cache()
				var model = this.train(dataPartition, context)
				// dataPartition.unpersist()
				//instrument model with dummy column mapping
				model = this.instrumentModel(model, dummyColumnMapping)
				model
			}
			case None ⇒ throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
					"Cannot get data partition for given dataContainerID: %s".format(dataContainerID), null)
		}
		
	}

	//adding property to model
	def instrumentModel(model: T, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]): T

	def train(dataPartition: RDD[(Matrix, Vector)], context: ExecutionContext): T

}

abstract class AUnsupervisedTrainer[T <: TModel](
		val dataContainerID: String,
		val xCols: Array[Int])(implicit m: Manifest[T]) extends AExecutor[T](doPersistResult = true){
	override def runImpl(ctx: ExecutionContext) = train(dataContainerID, ctx)
	/*
	 * Get an Option(RDD[DataPoint]) from dataContainerID in context
	 */
	def getDataPartition(dataContainerID: String, xCols: Array[Int], context: ExecutionContext): Option[RDD[Array[Double]]] = {
		Option(context.sparkThread.getDataManager.get(dataContainerID)) match {
			case Some(dataContainer) => dataContainer match{
				case sdf: SharkDataFrame => {
					/*
					Catch any column's type that currently not supporting and throwing an exception.
					Throwing exception in driver's program will result in returning meaningfull error message
					to client
					*/
					val xMetaInfo = for{
						idx <- xCols
					}yield(sdf.getMetaInfo.apply(idx))

					xMetaInfo.find(x => x.getType != "double" && x.getType != "int") match {
						case Some(x) =>  throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_COLUMN_TYPE, 
								"Column %s has unsupported type : %s".format(x.getHeader, x.getType), null)
						case None 	 =>
					}

					Option(sdf.getDataPointTable(xCols))
				}

				case df: DataFrame => {

					val xMetaInfo = for{
						idx <- xCols
					}yield(df.getMetaInfo.apply(idx))

					xMetaInfo.find(x => x.getType != "java.lang.Double" && x.getType != "java.lang.Int") match {
						case Some(x) =>  throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_COLUMN_TYPE, 
								"Column %s has unsupported type : %s".format(x.getHeader, x.getType), null)
						case None 	 =>
					}

					Option(df.getRDD.rdd) match {
						case Some(rdd) => Option(rdd.map(new ParsePoint(xCols, true)).filter(x => x!=null).cache())
						case None => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"Cannot getRDD for given dataContainerId: %s".format(dataContainerID), null)
					}
				}
			}
			case None => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
								"dataContainerID %s doesn't exist in user session".format(dataContainerID), null)
		}
	}
	def train(dataContainerID: String, context: ExecutionContext): T = {
		this.getDataPartition(dataContainerID, xCols, context) match {
			case Some(dataPartition) ⇒ {

				val model = this.train(dataPartition, context)
				// dataPartition.unpersist()
				model
			}
			case None ⇒ throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, 
					"Cannot get data partition for given dataContainerID: %s".format(dataContainerID), null)
		}
	}
	def train(dataPartition: RDD[Array[Double]], context: ExecutionContext): T
}

/**
 * Entry point for SparkThread executor to execute predictions
 */
abstract class APredictionExecutor[ResultType](implicit m: Manifest[ResultType]) extends AExecutor[ResultType] {
	protected override def runImpl(context: ExecutionContext): ResultType = this.predict

	/**
	 * We may want to pass context to [[predict]] as well, in case we want to refer to
	 * model or model IDs already known in the cluster. This helps avoid passing model
	 * parameters back-and-forth between client and server.
	 */
	def predict: ResultType
}
