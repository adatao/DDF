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

import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
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
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.RocMetric
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

class ROC(dataContainerID: String, xCols: Array[Int], var alpha_length: Int) extends AExecutor[RocMetric] {

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
		
		
		
		val ddfId = "SparkDDF_spark_" + dataContainerID.replaceAll("-", "_")
		
		
		println(">>>>>>>>>>>>>>>> ROC get ddf id=" + dataContainerID + "\tddfId=" + ddfId)
		
		val predictionDDF: DDF = ddfManager.getDDF((ddfId));
		predictionDDF.getMLMetricsSupporter().roc(predictionDDF, alpha_length)
	}

	/*
	 * map partition, one partition reduce to Array[Array[Double]]
	 */
	//	def getData(inputRows: Iterator[Array[Double]]): Iterator[Array[Array[Double]]] = {
	//		val rows = new ListBuffer[Array[Double]]
	//		var numRows = 0
	//		while (inputRows.hasNext) {
	//			val aRow = inputRows.next
	//			if (aRow != null) {
	//				rows.append(aRow)
	//				numRows += 1
	//			}
	//		}
	//		val X = new Array[Array[Double]](numRows)
	//		var row = 0
	//		rows.foreach(inputRow ⇒ {
	//			X(row) = Array(inputRow(0), inputRow(1))
	//			row += 1
	//		})
	//		Iterator(X)
	//	}
	//
	//	/*
	//	 */
	//	def checkBinaryClassification(inputRows: Iterator[Array[Double]]): Iterator[Array[Int]] = {
	//		val isBinary = new Array[Int](1)
	//		val isNotBinary = new Array[Int](1)
	//
	//		isBinary(0) = 1
	//		isNotBinary(0) = 0
	//
	//		var numRows = 0
	//		val YTRUE_INDEX = 0
	//		while (inputRows.hasNext) {
	//			val aRow = inputRows.next
	//			if (aRow != null) {
	//				if (aRow(YTRUE_INDEX) != 0 && aRow(YTRUE_INDEX) != 1) {
	//					println(">>>isNotBinary=" + isNotBinary(0))
	//					return (Iterator(isNotBinary))
	//				}
	//			}
	//		}
	//		println(">>>isNotBinary=" + isBinary(0))
	//		return (Iterator(isBinary))
	//	}
}
