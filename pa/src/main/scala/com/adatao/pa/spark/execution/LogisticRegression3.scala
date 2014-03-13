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
import java.util.Arrays

import scala.Array.canBuildFrom
import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.adatao.pa.spark.DataManager
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.DataManager.SharkDataFrame
import com.adatao.pa.spark.SparkThread
import com.adatao.pa.spark.types.ExecutorResult
import com.adatao.pa.spark.types.SuccessResult
import shark.api.Row
import org.apache.spark.rdd.RDD

class Unused_LogisticRegression3  extends CExecutor {
  
	// !!! Client-API Variables. If you change these, also change the client code.

	var dataContainerID: String = ""
	var xCols: Array[Int] = null
	var yCol: Int = 0
	var learningRate: Double = 0.0
	var numIters: Int = 0
	var initialWeights: Array[Double] = null

	// !!! Client-API Variables. If you change these, also change the client code.
	class LogisticRegressionResult(w: Array[Double], tError: Array[Double]) extends SuccessResult {
	  var weights: Array[Double] = null
	  var trainingError: Array[Double] = null

	  weights = w
	  trainingError = tError
	  
	}

	/**
	 * main method
	 * input: normal data frame or shark dataframe
	 * invoke train method accordingly 
	 */
	override def run(sparkThread: SparkThread): ExecutorResult = {
		Option(sparkThread) match {
			case Some(thread) ⇒ Option(thread.getDataManager()) match {
				case Some(manager) ⇒ Option(manager.get(dataContainerID)) match {
					case Some(container) ⇒ Option(container.getType()) match {
					  case Some(ContainerType.DataFrame) => trainArrayObject(container.getRDD().rdd)
					  case Some(ContainerType.SharkDataFrame) => trainShark(container)
					  case _ ⇒ null
					}
					case _ ⇒ null
				}
				case _ ⇒ null
			}
			case _ ⇒ null
		}
	}
	
	/**
	 * handle shark data frame
	 * get shark dataframe, parse and invoke train method
	 */
	def trainShark(dataFrame: DataManager.DataContainer): LogisticRegressionResult = {
		if (initialWeights == null) {
			var rand = new Random(System.currentTimeMillis())
			initialWeights = new Array[Double](xCols.length + 1).map(_ ⇒ 2 * rand.nextDouble() - 1)
		}
		var sdf: SharkDataFrame = dataFrame.asInstanceOf[SharkDataFrame]
		var points: RDD[Array[Array[Double]]] = sdf.getTableRDD.rdd.map(new SharkParsePoint3(yCol, xCols, sdf.getMetaInfo))
		points.cache()
		train(points, initialWeights)
	}
	
	/**
	 * handle normal dataframe
	 * input: RDD array of object
	 * 
	 */
	def trainArrayObject(dataTable: RDD[Array[Object]]): LogisticRegressionResult = {
		if (initialWeights == null) {
			var rand = new Random(System.currentTimeMillis())
			initialWeights = new Array[Double](xCols.length + 1).map(_ ⇒ 2 * rand.nextDouble() - 1)
		}
		val points: RDD[Array[Array[Double]]] = dataTable.mapPartitions(prepareDataPartitionMapper(yCol, xCols))
		points.cache() // do we need this? Yes, apparently we do!
		
		train(points, initialWeights)
	}
	
	/**
	 * train method, take array[array[double]]
	 * will be invoked by both dataframe and sharkdataframe
	 */
	def train(points: RDD[Array[Array[Double]]], weights: Array[Double]) : LogisticRegressionResult = {
	  // Sanity check
		println("initial weights = %s".format(Arrays.toString(weights)))
		if (weights.length != xCols.length + 1) {
			throw new java.lang.IllegalArgumentException(
				"weights (%d) must equal numXCols (%d) + 1 (one xCol is the bias term)".format(
					initialWeights.length, xCols.length))
		}

		// HACK: rebase yCol and xCols[] here, because our code is base-0, but R client code is base-1.
		// Is better done at a higher client level.
		yCol = yCol-1
		var i = 0
		while( i < this.xCols.length) {
			xCols(i) = xCols(i) -1
			i = i+1
		}
		
		// Track training errors for all iterations, plus 1 more for the error before the first iteration
		val trainingError = new Array[Double](numIters + 1)
		val numWeights = weights.length
		var computationState: ComputationState = null
		var iter = 0
		while (iter < numIters + 1) {
		  
			computationState = points.map(computationStateMapper(weights)).reduce(_.addIn(_))
			trainingError(iter) = computationState.error / computationState.numSamples

			var multiplier = learningRate / computationState.numSamples

			// Update the weights, except for the last iteration
			if (iter <= numIters) {
			    //tempw = computationState.gradients.map(a => -multiplier*a)
			    var feature = 0
				while (feature < numWeights) {
					weights(feature) -= multiplier * computationState.gradients(feature)
					feature += 1
				}
			}
			iter += 1
		}

		println("final weights = %s".format(Arrays.toString(weights)))
		println("trainingError = %s".format(Arrays.toString(trainingError)))

		new LogisticRegressionResult(weights, trainingError)
	}
	
	/**
	 * partition mapper
	 * do the transform from Iterator[Array[object]] to one big single array[array[double]] 
	 * this function will handle normal dataframe from HDFS files
	 */
	def prepareDataPartitionMapper(yCol: Int, xCols: Array[Int])(inputRows: Iterator[Array[Object]]): Iterator[Array[Array[Double]]] = {
		val outputTable: ListBuffer[Array[Double]] = new ListBuffer[Array[Double]]()

		var inputRow: Array[Object] = null
		var outputRow: Array[Double] = null
		val numCols = xCols.length + 2 // 1 bias term + n x-features + 1 y-value

		while (inputRows.hasNext) {
			inputRow = inputRows.next
			if (inputRow != null) { // skip missing row

				outputRow = Array.ofDim[Double](numCols)
				outputRow(0) = 1.0 // bias term				
				outputRow(numCols - 1) = inputRow(yCol - 1).asInstanceOf[Double] // y-value
				var i = 1
				while (i < numCols - 1) {
					outputRow(i) = inputRow(xCols(i - 1)).asInstanceOf[Double] // x-feature #i
					i += 1
				}

				outputTable.append(outputRow)
			}
		}

		Iterator(outputTable.toArray)
	}
	
	/**
	 * computationState
	 * store all computation result at each iterator so far
	 * gradients: gradients value each of each features
	 * errors: accumulate errors so far
	 * numSamples: accumulate number of sample sofar 
	 */
	class ComputationState(weights: Array[Double]) extends Serializable {

		var gradients = new Array[Double](weights.length)
		var error: Double = 0.0
		var numSamples: Long = 0

		override def toString: String = {
			"ComputationState: weights = %s\tgradients = %s\terror = %.4f\tnumSamples = %d".format(
				Arrays.toString(weights), Arrays.toString(gradients), error, numSamples)
		}

		
		def numWeights = Option(weights) match {
			case Some(w) ⇒ w.length
			case _ ⇒ 0
		}

		/**
		 * Sum in-place to avoid new object alloc
		 */
		def addIn(other: ComputationState): ComputationState = {
			// Sum the gradient vectors column-by-column
			var i: Int = 0
			while (i < numWeights) {
				gradients(i) += other.gradients(i)
				i += 1
			}
			
			// Sum the error values, which are assumed to be associative 
			error = math.abs(error) + math.abs(other.error)
			numSamples += other.numSamples

			this
		}
	}
  
	/**
	 * mapper to compute computationstate 
	 */
	def computationStateMapper(weights: Array[Double])(inputRows: Array[Array[Double]]): ComputationState = {
		val computationState: ComputationState = new ComputationState(weights)

		val numRows = inputRows.length
		val numWeights = weights.length
		var sampleError: Double = 0.0
		var h: Double = 0.0;
		var totalError: Double = 0.0
		var feature: Array[Double] = null

		// This is the most important loop to optimize, since it runs once per data row
		var row: Int = 0
		var column: Int = 0
		while (row < numRows) {
			feature = inputRows(row)
			//need to reset h, after each row
			column = 0
			h = 0
			while (column < numWeights) {
				h += weights(column) * feature(column)
				column += 1
			}
			h = 1 / (1 + math.exp(-h))
			
			// update sample error, logit function, sample error = delta = h - y
			sampleError = h  -feature(numWeights)
			
			// Accumulate the gradients
			column = 0
			while (column < numWeights) {
				computationState.gradients(column) += sampleError * feature(column)
				column += 1
			}

			// Accumulate the error, use Andrew Ng cost function, coursera
			totalError = math.abs(totalError) + math.abs(feature(numWeights) * math.log(h) + (1 - feature(numWeights)) * math.log(1 - h));

			row += 1
		}

		computationState.numSamples = numRows
		computationState.error = totalError

		computationState
	}
	
	/**
	 * Parsepoint class for shark rows
	 */
	class SharkParsePoint3(ycol: Int, xCols: Array[Int], metaInfo: Array[DataManager.MetaInfo]) extends Function1[Row, Array[Array[Double]]]
	  with Serializable
	  {
		private val base0_YCol: Int = ycol - 1
	    private val base0_XCols: Array[Int] = new Array[Int](xCols.length)
	    var i = 0
	    while(i < base0_XCols.length){
	      base0_XCols(i) = xCols(i) - 1
	      i = i + 1
	    }
	    
	    override def apply(row: Row): Array[Array[Double]] = row match {
	      case null =>  null
	      case x => {
	        val dim = base0_XCols.length
	        val result: Array[Double] = new Array[Double](dim + 2)
	        val arr: Array[Array[Double]] = new Array[Array[Double]](1)
	        //bias term
	        result(0) = 1.0
	        var i = 1
	        while(i < dim + 1){
	          metaInfo(i-1).getType() match{
	            case "int" => row.getInt(base0_XCols(i-1)) match{
	              case null => return null
	              case x =>    result(i) = x.asInstanceOf[Double]
	            }
	            case "bigint" => row.getInt(base0_XCols(i-1)) match{
	              case null => return null
	              case x =>    result(i) = x.asInstanceOf[Double]
	            }
	            case "double" => row.getDouble(base0_XCols(i-1)) match{
	              case null => return null
	              case x => result(i) = x
	            }
	              i = i + 1
	          }
	        }
			var yType: String = metaInfo(base0_YCol).getType();
			if(yType.equals("int")) result(dim + 1) = row.getInt(base0_YCol).toDouble;
			if(yType.equals("double")) result(dim + 1) = row.getDouble(base0_YCol).asInstanceOf[Double]
			
			arr(0) = result
	        arr
	      }
	    }
	  }
}
