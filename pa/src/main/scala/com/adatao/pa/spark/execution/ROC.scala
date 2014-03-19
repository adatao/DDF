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

import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.ML.TModel
import com.adatao.ML.LogisticRegressionModel
import com.adatao.ML.{Utils => MLUtils}
import com.adatao.ML.spark.RddUtils
import java.util.ArrayList
import org.jblas.DoubleMatrix
import org.apache.spark.rdd.RDD
import com.adatao.ML.spark.RocObject
import com.adatao.ML.spark.Metrics
import scala.collection.mutable.ListBuffer
import com.adatao.ML.ALinearModel
import com.adatao.ML.LinearRegressionModel
import java.util.HashMap
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * From ctn: What is the intended API for ROC? Are we passing in a model and have it generate predictions, then compute
 * statistics for those prediction; or are we passing in predictions and expect it only to compute statistics for those
 * predictions?
 * 
 * Khang: it's the former. Get the data, do prediction, then compute statistic. 
 * 
 */

class ROC(dataContainerID: String, xCols: Array[Int], var alpha_length: Int) extends AUnsupervisedTrainer[LinearRegressionModel](dataContainerID, xCols) {

	def train(dataPartition: RDD[Array[Double]], ctx: ExecutionContext): RocObject = {
			//first check if if input data is binary classification
			val YTRUE_INDEX = 0
			val df = ctx.sparkThread.getDataManager.get(dataContainerID)
			val metaInfos= df.getMetaInfo
			var isBinaryLabel = true
			var isEmpty = false
			
			//if yTrue has factor
			if(metaInfos != null && metaInfos(YTRUE_INDEX) != null && metaInfos(YTRUE_INDEX).hasFactor()) {
				val yTrueIterator = metaInfos(YTRUE_INDEX).getFactor().keySet().iterator()
				while(isBinaryLabel && yTrueIterator.hasNext()) {
					val value = yTrueIterator.next()
					if(value != "0" && value != "1")
						isBinaryLabel = false
				}
			}
			//if yTrue is not factor
			else {
				val isBinaryClassification = dataPartition.mapPartitions(checkBinaryClassification).collect
				var i = 0
				while (isBinaryLabel && i < isBinaryClassification.length) {
					var j = 0
					while (isBinaryLabel && j < isBinaryClassification(i).length) {
						if(isBinaryClassification(i)(j) != 0 && isBinaryClassification(i)(j) != 1) {
							isBinaryLabel = false
						}
						j += 1
					}
					i += 1
				}
				if (isBinaryClassification == null  || isBinaryClassification.length == 0) {
					isEmpty = true
				}
			}
			
			if(isEmpty) {
				if(metaInfos == null)
					LOG.error("Predicted data is empty and metaInfos is null");
				else 
					LOG.error("Predicted data is empty and metaInfos =" + metaInfos);
				throw new AdataoException(AdataoExceptionCode.ERR_ROC_EMPTY, "Please check if predicted data is empty.", null);
			}
			if(!isBinaryLabel) {
				LOG.error("True label data is not binary classified data, please check input data");
				throw new AdataoException(AdataoExceptionCode.ERR_ROC_NOT_BINARY, "Please make sure input data is binary classified.", null);
			}
			val data = dataPartition.mapPartitions(getData)
			Metrics.ROC(data, alpha_length)
	}

	/*
	 * map partition, one partition reduce to Array[Array[Double]]
	 */
	def getData(inputRows: Iterator[Array[Double]]): Iterator[Array[Array[Double]]] = {
		val rows = new ListBuffer[Array[Double]]
		var numRows = 0
		while (inputRows.hasNext) {
			val aRow = inputRows.next
					if (aRow != null) {
						rows.append(aRow)
						numRows += 1
					}
		}
		val X = new Array[Array[Double]](numRows)
		var row = 0
		rows.foreach(inputRow ⇒ {
			X(row) = Array(inputRow(0), inputRow(1)) 
					row += 1
		})
		Iterator(X)
	}
	
	/*
	 */
	def checkBinaryClassification(inputRows: Iterator[Array[Double]]): Iterator[Array[Int]] = {
		val isBinary = new Array[Int](1)
		val isNotBinary = new Array[Int](1)

		isBinary(0) = 1
		isNotBinary(0) = 0

		var numRows = 0
		val YTRUE_INDEX = 0
		while (inputRows.hasNext) {
			val aRow = inputRows.next
			if (aRow != null) {
				if(aRow(YTRUE_INDEX) != 0 &&  aRow(YTRUE_INDEX) != 1 ){
					println(">>>isNotBinary=" + isNotBinary(0))
					return (Iterator(isNotBinary))
				}
			}
		}
		println(">>>isNotBinary=" + isBinary(0))
		return (Iterator(isBinary))
	}
}
