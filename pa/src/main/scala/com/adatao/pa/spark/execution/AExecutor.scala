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
import io.ddf.types.Matrix
import io.ddf.types.Vector
import com.adatao.ML._
import com.adatao.pa.spark.SparkThread
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.DataManager
import com.adatao.pa.spark.DataManager.{ SharkDataFrame, DataFrame }
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessfulResult
import com.adatao.pa.spark.types.FailedResult
import io.spark.ddf.util.MLUtils.ParsePoint
import scala.Some
import com.adatao.pa.spark.types.ExecutionException
import scala.reflect.Manifest
import scala.collection.JavaConversions._
//import com.adatao.ML.spark.Predictions
//import com.adatao.ML.spark.TransformDummyCoding
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import java.util.HashMap
import org.apache.spark.api.java.JavaSparkContext
import io.ddf.DDFManager
import io.ddf.DDF
import io.ddf.ml.Model

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
			new SuccessfulResult(this.runImpl(context))
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

	//TODO remove change in this class

	def train(dataContainerID: String, context: ExecutionContext): T = {
	  null.asInstanceOf[T]

	}
	/*
	 * project data before modelling
	 */
	def project(ddf: DDF): DDF = {
	  //get projection
    var columnList: java.util.List[java.lang.String] = new java.util.ArrayList[java.lang.String]
    //project on xCols
    for (col ← xCols) columnList.add(ddf.getSchema().getColumn(col).getName)
    //project on yCol
    columnList.add(ddf.getSchema().getColumn(yCol).getName)
    val projectedDDF = ddf.Views.project(columnList)
    projectedDDF
	}
	

	//adding property to model
//	def train(dataPartition: RDD[(Matrix, Vector)], context: ExecutionContext): T

}

abstract class AUnsupervisedTrainer[T <: TModel](
		val dataContainerID: String,
		val xCols: Array[Int])(implicit m: Manifest[T]) extends AExecutor[T](doPersistResult = true) {
//	override def runImpl(ctx: ExecutionContext) = train(dataContainerID, ctx)
	/*
	 * Get an Option(RDD[DataPoint]) from dataContainerID in context
	 */
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
