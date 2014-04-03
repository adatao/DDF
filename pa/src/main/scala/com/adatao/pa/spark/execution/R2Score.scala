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
		//all we need is to change it HERE
		//old API
//		val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)
		
		val mymodel: IModel = ddfManager.getModel(modelID)
		val resultDDF = ddf.getMLSupporter().applyModel(mymodel, true, true)
		val predictionsRDD = resultDDF.asInstanceOf[SparkDDF].getRDD(classOf[Tuple2[Double, Double]])
		
		//get column mean
		val summary = ddf.getStatisticsSupporter().getSummary()
		val yMean = summary(yCol).mean()
		
		Metrics.R2Score(predictionsRDD, yMean)

		// then compute R2 score
		//old code
//		Option(ctx.sparkThread.getDataManager.get(dataContainerID)) match {
//			case Some(dc) ⇒ Option(dc.getColumnMean(yCol)) match {
//				case Some(yMean) ⇒ Metrics.R2Score(predictions, yMean) // already have yMean
//				case _ ⇒ Metrics.R2Score(predictions)
//			}
//			case _ ⇒ Metrics.R2Score(predictions)
//		}
	}
}
