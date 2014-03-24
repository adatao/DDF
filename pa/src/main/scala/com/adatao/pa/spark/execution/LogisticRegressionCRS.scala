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

import java.lang.String
import com.adatao.ML
import com.adatao.ML.Utils
import com.adatao.ML.TModel
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import com.adatao.ML.ALossFunction
import com.adatao.spark.RDDImplicits._
import java.util.HashMap
import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.CompRowMatrix
import com.adatao.ddf.types.MatrixSparse
import org.jblas.MatrixFunctions
import com.adatao.ML.GradientDescent
import scala.util.Random
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary
import com.adatao.ddf.DDFManager
import com.adatao.pa.spark.SparkThread
import com.adatao.pa.spark.types.ExecutorResult
import com.adatao.ddf.DDF
import com.adatao.ddf.exception.DDFException
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessResult
import com.adatao.ddf.ml.IModel


 class LogisticRegressionCRSResult (model: LogisticRegressionModel) extends SuccessResult {
 }


/**
 * Entry point for SparkThread executor
 */
class LogisticRegressionCRS(
	dataContainerID: String,
	xCols: Array[Int],
	yCol: Int,
	columnsSummary: HashMap[String, Array[Double]],
	var numIters: Int,
	var learningRate: Double,
	var ridgeLambda: Double,
	var initialWeights: Array[Double]) {
	
  var ddfManager: DDFManager = null

	  
	def run(sparkThread: SparkThread): IModel = {
		ddfManager = sparkThread.getDDFManager();
    val ddf: DDF  = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"))
    try {

    	val regressionModel = ddf.ML.train("LogisticRegressionCRS", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeights.toArray)
      
      return (regressionModel)
    } catch  {
    	case ioe: DDFException  => throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, ioe.getMessage(), null);
    }
	}
	
//	def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): LogisticRegressionModel = {
//		
//		
//		val regressionModel = ddfTrain2.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
//      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
//		
//		println(">>>> columnsSumary size ==" + columnsSummary.get("min").size + "\telement(0) min=" + columnsSummary.get("min")(0) + "\tmax=" + columnsSummary.get("max")(0))
//		val SPARSE_RANGE = Integer.parseInt(System.getProperty("sparse.max.range", "10024"))
//		println("SPARSE_RANGE=" + SPARSE_RANGE)
//		
//		//build sparse columns: column index, Array(min, max)
//		//for example, ["1", [120, 10000]]
//		
//		//build column start index map
//		var sparseColumns = new HashMap[Int, Array[Double]]()
//		var sparseColumnsPaddingIndex = new HashMap[Int, Int]()
//		
//		//get new number of sparse columns
//		var i = 0 
//		var sumAllRange = 0
//		while(i < columnsSummary.get("min").size) {
//			val range = if (columnsSummary.get("max")(i) > columnsSummary.get("min")(i))  columnsSummary.get("max")(i) - columnsSummary.get("min")(i) else  0
//			if(range >= SPARSE_RANGE) {
//
//				sparseColumns.put(i, Array(columnsSummary.get("min")(i), columnsSummary.get("max")(i)))
//				sparseColumnsPaddingIndex.put(i, sumAllRange)
//				
//				println(">>>> sparsecolumn = " + i + "\tpadding = " + sparseColumnsPaddingIndex.get(i))
//				
//				sumAllRange += range.asInstanceOf[Int] + 1
//				
//			}
//			i += 1
//		}
//		
//		//create transformer object
//	}
	
	
//	//post process, set column mapping to model
//	def instrumentModel(model: LogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) :LogisticRegressionModel = {
//	  model.dummyColumnMapping = mapping
//	  model
//	}
}

