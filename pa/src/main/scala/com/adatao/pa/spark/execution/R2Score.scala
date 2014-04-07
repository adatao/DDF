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

package com.adatao.pa.spark.execution

import com.adatao.ML.{ Utils ⇒ MLUtils, _ }
import com.adatao.ML.spark.{ Metrics, RddUtils }
import scala.Some
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.spark.ddf.SparkDDF

/**
 *
 */
class R2Score(var dataContainerID: String, val xCols: Array[Int], val yCol: Int, var modelID: String) extends AExecutor[Double] {

	def runImpl(ctx: ExecutionContext): Double = {
		val ddfManager = ctx.sparkThread.getDDFManager();
		val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));

		// first, compute RDD[(ytrue, ypred)]
		//old API
		//		val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)

		
		println(">>>>> R2score input rdd name = " + ddf.getName() + "\t ddf Uri=" + ddf.getUri())
		val mymodel: IModel = ddfManager.getModel(modelID)
		require(mymodel != null)
		println(">>>>> R2score mymodel = " + mymodel.getName() + "\t ddf Uri=" + ddf.getUri())

		
		val predictionDDF = ddf.getMLSupporter().applyModel(mymodel, true, true)
		//		val predictionsRDD = predictionDDF.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]])
		//get column mean
		val summary = ddf.getStatisticsSupporter().getSummary()
		val yMean = summary(yCol).mean()
//		println(">>>>> predictionDDF id = " + predictionDDF.getName() + "\t predictionDDF Uri=" + predictionDDF.getUri() + ">>> yMean=" + yMean)
		

		ddf.getMLMetricsSupporter().r2score(predictionDDF, yMean)

		//		Metrics.R2Score(predictionsRDD, yMean)

	}
}
