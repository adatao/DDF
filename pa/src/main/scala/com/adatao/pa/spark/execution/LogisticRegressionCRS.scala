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

package adatao.bigr.spark.execution

import java.lang.String
import adatao.ML
import adatao.ML.Utils
import adatao.ML.TModel
import adatao.ML.types.Matrix
import adatao.ML.types.Vector
import org.apache.spark.rdd.RDD
import adatao.ML.LogisticRegressionModel
import adatao.ML.ALossFunction
import adatao.spark.RDDImplicits._
import java.util.HashMap
import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.CompRowMatrix
import adatao.ML.types.MatrixSparse
import org.jblas.MatrixFunctions
import adatao.ML.GradientDescent
import scala.util.Random
import adatao.bigr.spark.execution.FiveNumSummary.ASummary

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
	var initialWeights: Array[Double])
		extends AModelTrainer[LogisticRegressionModel](dataContainerID, xCols, yCol) {

	
	def train(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): LogisticRegressionModel = {
		
		
		println(">>>> columnsSumary size ==" + columnsSummary.get("min").size + "\telement(0) min=" + columnsSummary.get("min")(0) + "\tmax=" + columnsSummary.get("max")(0))
		val SPARSE_RANGE = Integer.parseInt(System.getProperty("sparse.max.range", "10024"))
		println("SPARSE_RANGE=" + SPARSE_RANGE)
		
		//build sparse columns: column index, Array(min, max)
		//for example, ["1", [120, 10000]]
		
		//build column start index map
		var sparseColumns = new HashMap[Int, Array[Double]]()
		var sparseColumnsPaddingIndex = new HashMap[Int, Int]()
		
		//get new number of sparse columns
		var i = 0 
		var sumAllRange = 0
		while(i < columnsSummary.get("min").size) {
			val range = if (columnsSummary.get("max")(i) > columnsSummary.get("min")(i))  columnsSummary.get("max")(i) - columnsSummary.get("min")(i) else  0
			if(range >= SPARSE_RANGE) {

				sparseColumns.put(i, Array(columnsSummary.get("min")(i), columnsSummary.get("max")(i)))
				sparseColumnsPaddingIndex.put(i, sumAllRange)
				
				println(">>>> sparsecolumn = " + i + "\tpadding = " + sparseColumnsPaddingIndex.get(i))
				
				sumAllRange += range.asInstanceOf[Int] + 1
				
			}
			i += 1
		}
		
		//create transformer object
		val transformer: TransformSparseMatrix = new TransformSparseMatrix (sparseColumns, sparseColumnsPaddingIndex, sumAllRange)
		//convert to MatrixSparse
		val sparseDataPartition = transformer.transform(dataPartition, ctx)
		trainSparse(sparseDataPartition, sumAllRange, ctx)
	}
	
	def trainSparse(dataPartition: RDD[(MatrixSparse, Vector)], sumAllRange: Int, ctx: ExecutionContext): LogisticRegressionModel = {
		
		val newColumnRange = sumAllRange
		
		var weights = null.asInstanceOf[Vector]
		if(sumAllRange > 0)  
			weights = Utils.randWeights(numFeatures + sumAllRange) //Vector(initialWeights)
		else weights = if (initialWeights == null || initialWeights.length != numFeatures)  Utils.randWeights(numFeatures)  else Vector(initialWeights) 
		
		
		var lossFunction = new LogisticRegressionCRS.LossFunction(dataPartition, ridgeLambda)
		val (finalWeights, trainingErrors, numSamples) = GradientDescent.runBatch(lossFunction, getWeights(Option(weights), numFeatures), learningRate, numIters)
		
		new LogisticRegressionModel(finalWeights, trainingErrors, numSamples)
		
	}
	
	// Generate random weights if necessary
	private def getWeights(initialWeights: Option[Vector], numFeatures: Int /* used only if initialWeights is null */ ): Vector = {
		initialWeights match {
			case Some(vector) ⇒ vector
			case None ⇒ Vector(Seq.fill(numFeatures)(Random.nextDouble).toArray)
		}
	}
	
	//post process, set column mapping to model
	def instrumentModel(model: LogisticRegressionModel, mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) :LogisticRegressionModel = {
	  model.dummyColumnMapping = mapping
	  model
	}
}

/*
 * transform RDD[(Matrix, Vector)] to RDD[(MatrixSparse, Vector)] 
 */
class TransformSparseMatrix (sparseColumns: HashMap[Int, Array[Double]], sparsePaddingIndex: HashMap[Int, Int], sumAllRange:Int) extends Serializable {
	
	//TO DO: convert to MatrixSparse
	def transform(dataPartition: RDD[(Matrix, Vector)], ctx: ExecutionContext): (RDD[(MatrixSparse, Vector)]) = {
		//TO DO: delete this
		dataPartition.map(transformSparse(this.sparseColumns))
	}
	
	//input Matrix,
	//output: MatrixSparse
	//example: 
	
	
	//Input matrix: 2 rows, 3 columns and we want to represent 2th column in sparse format
	
	//header: mobile_hight, advertise_id, mobile_width 
	//[10, 20, 30]
	//[15, 134790, 25]
	
	//output matrix: 2 rows, new sparse columns
	//[10, 0, 30, 0, 0, 0, ....., 1] all are zero and the 23th column will be represent = 1
	//[15, 0, 25, 0, 0, 0, ......., 1] all are zero and the (134790 + 3)th column will be represent = 1
	
	//when we represent this the weight for each advertise_id will represent by calling: w[23], and w[134793] respectively	
	//if this presentation is suitable then all the formular in logistic regression will be the same with formular in the dense matrix case
	

	//TO DO: convert to MatrixSparse
	def transformSparse (sparseColumns: HashMap[Int, Array[Double]])(inputRows: (Matrix, Vector)): (MatrixSparse, Vector) = {
		val X = inputRows._1
		val Y = inputRows._2
		
		val maxOriginColumns = X.getColumns()
		
		println(">>> transformSparse with sparseColumns=" + sparseColumns)

		var Xprime: MatrixSparse = null
		//TO DO: convert to sparse
		if(sparseColumns != null && sparseColumns.size() > 0) {
			Xprime = new MatrixSparse(X.getRows(), maxOriginColumns + sumAllRange)
		}
		else {
			Xprime = new MatrixSparse(X.getRows(), maxOriginColumns)
		}
		
		val defaultValue = 1.0
		//fill in
		var row = 0
		var column = 0
		var newColumn = 0
		while( row < X.getRows()) {
			column = 0
			while (column < X.getColumns()) {
				val currentCell = X.get(row ,column)
				if(sparseColumns != null && sparseColumns.size() > 0 && sparseColumns.containsKey(column)) {
					//check if this column is sparse column
					if(!sparseColumns.containsKey(column)) {
						//set as normal
						Xprime.crs.set(row, column, currentCell)
					}
					//sparse column meaning, the cell value indicate the new  column index
					else {
						//based 0, new column index = number original columns + padding index + internal column index
						newColumn = 	maxOriginColumns
						newColumn +=  	sparsePaddingIndex.get(column) 
						newColumn += 	currentCell.asInstanceOf[Int] - 1
						
						
						//offset by minmum cell value
						newColumn = newColumn - sparseColumns.get(column).min.asInstanceOf[Int] + 1
						
						
						Xprime.crs.set(row, newColumn, defaultValue)
//						println(">>> origincolumn=" + column + "\tpadding=" + sparsePaddingIndex.get(column) + "\tcellValue=" + currentCell.asInstanceOf[Int] + "\tnewcolumn=" + newColumn)
					}
				}
				else {
					//set as normal
					Xprime.crs.set(row, column, currentCell)
					
				}
				column += 1
			}
			
			row += 1
		}
		
		(Xprime, Y)
	}
}
	
object LogisticRegressionCRS {
	class LossFunction(@transient XYData: RDD[(MatrixSparse, Vector)], ridgeLambda: Double) extends ALossFunction {
		def compute: Vector ⇒ ALossFunction = {
			(weights: Vector) ⇒ XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.safeReduce(_.aggregate(_))
		}
		
		def computeHypothesis(X: MatrixSparse, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
			
			val startTime = System.currentTimeMillis()
			
			val linearPredictor = this.computeLinearPredictor(X, weights)
			
			val endTime = System.currentTimeMillis()
			println(">>>>>>>>>>>>>>>>>> timing  computeHypothesis: " + (endTime-startTime))
			
			
			(linearPredictor, ALossFunction.sigmoid(linearPredictor))
			
		}
		
		def computeLoss(Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix) = {
			
			val startTime = System.currentTimeMillis()
			val YT = Y.transpose()
			val lossA: Double = Y.dot(ALossFunction.safeLogOfSigmoid(hypothesis, linearPredictor)) // Y' x log(h)
			val lossB: Double = Vector.fill(Y.length, 1.0).subi(Y).dot(
				ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), linearPredictor.neg)
			) // (1-Y') x log(1-h)
			
			
			var J = -(lossA + lossB)
			if (ridgeLambda != 0.0) J += (ridgeLambda / 2) * weights.dot(weights)
			
			val endTime = System.currentTimeMillis()
			println(">>>>>>>>>>>>>>>>>> timing computeLoss: " + (endTime-startTime))
			
			J
		}
		
		final def computeLinearPredictor(X: MatrixSparse, weights: Vector): DoubleMatrix = {
			X.mmul(weights)
		}
	
		protected def computeGradients(X: MatrixSparse, weights: Vector, errors: DoubleMatrix): Vector = {
			val startTime = System.currentTimeMillis()
			
//			val gradients = Vector(errors.transpose().mmul(X)) // (h - Y) x X = errors.transpose[1 x m] * X[m x n] = [1 x n] => Vector[n]
			//mmul2 is reverse order of mmul, meaning errors * X
			val temp = X.mmul2(errors.transpose())
			
			var endTime2 = System.currentTimeMillis()
			println(">>>>>>>>>>>>>>>>>> timing computeGradients middle: " + (endTime2-startTime))
			
			val gradients = Vector(temp.getData()) // (h - Y) x X = errors.transpose[1 x m] * X[m x n] = [1 x n] => Vector[n]
			if (ridgeLambda != 0.0) gradients.addi(weights.mul(ridgeLambda)) // regularization term, (h - Y) x X + L*weights
			
			
			var endTime = System.currentTimeMillis()
			println(">>>>>>>>>>>>>>>>>> timing computeGradients: " + (endTime-startTime))
			
			
			gradients
		}
	
		def compute(X: MatrixSparse, Y: Vector, theWeights: Vector): ALossFunction = {
			
			val (linearPredictor, hypothesis) = this.computeHypothesis(X, theWeights)
			val errors = hypothesis.sub(Y)
			
			gradients = this.computeGradients(X, theWeights, errors)
			
			loss = this.computeLoss(Y, theWeights, errors, linearPredictor, hypothesis)
			
			weights = theWeights
			numSamples = Y.rows
			
			this
		}
		
		//not used
		def compute(X: Matrix, Y: Vector, weights: Vector): ALossFunction = {
			
			println(">>>>>>>>>>>> old method")
			null
		}
		
	}
}
