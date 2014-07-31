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

import io.ddf.types.Matrix
import io.ddf.types.Vector
import com.adatao.ML.TModel
import com.adatao.ML.LogisticRegressionModel
import com.adatao.ML.{ Utils ⇒ MLUtils }
import com.adatao.ML.spark.RddUtils
import java.util.ArrayList
import org.jblas.DoubleMatrix
import org.apache.spark.rdd.RDD
//import com.adatao.ML.spark.RocObject
//import com.adatao.ML.spark.Metrics
import scala.collection.mutable.ListBuffer
import com.adatao.ML.ALinearModel
import com.adatao.ML.LinearRegressionModel
import java.util.HashMap
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import io.ddf.DDF
import io.ddf.ml.RocMetric
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessfulResult
import com.adatao.pa.spark.types.FailedResult
import com.adatao.pa.spark.types.ExecutionException

/**
 * From ctn: What is the intended API for ROC? Are we passing in a model and have it generate predictions, then compute
 * statistics for those prediction; or are we passing in predictions and expect it only to compute statistics for those
 * predictions?
 *
 * Khang: it's the former. Get the data, do prediction, then compute statistic.
 *
 */

class ROC(dataContainerID: String, alpha_length: Int) extends AExecutor[RocMetric] {

	override def run(ctx: ExecutionContext): ExecutionResult[RocMetric] = {
		try {
			val result = new SuccessfulResult(this.runImpl(ctx))
			//      if (doPersistResult) result.persistenceID = context.sparkThread.getDataManager.putObject(result.result)
			result
		}
		catch {
			case ee: ExecutionException ⇒ new FailedResult[RocMetric](ee.message)
		}
	}

	override def runImpl(ctx: ExecutionContext): RocMetric = {
		//first check if if input data is binary classification
		//TODO double check if ytrueypred back by table i.e has schema
		//    val df = ctx.sparkThread.getDataManager.get(dataContainerID)
		val ddfManager = ctx.sparkThread.getDDFManager()
    LOG.info(">>> dataContainerID = " + dataContainerID)
		val predictionDDF: DDF = ddfManager.getDDF(dataContainerID);
    if(predictionDDF.getMLMetricsSupporter == null) {
      LOG.info(">>>>>> MLMetricsSupporter is null")
    } else {
      LOG.info(">>>>>> MLMetricsSupporter is not null")
    }
		predictionDDF.getMLMetricsSupporter().roc(predictionDDF, alpha_length)
	}
}
