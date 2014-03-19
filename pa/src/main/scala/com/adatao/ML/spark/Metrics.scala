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

package com.adatao.ML.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.adatao.ML.{ Utils ⇒ MLUtils, _ }
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.spark.RDDImplicits._
import java.util.Arrays

// @author: aht

class RocObject(var pred: Array[Array[Double]], var auc: Double) extends LinearRegressionModel(new Vector(1), new Vector(1), 0) {

	def print() {
		var i: Int = 0
		while (i < this.pred.length) {
			if (this.pred(i) != null) {
				println(">>>\t" + i + "\t" + Arrays.toString(this.pred(i)))
			}
			i = i + 1
		}
		println(">>>>> auc=" + auc)
	}
	def computeAUC(): Double = {
		//filter null/NA in pred
		var i: Int = 0
		var previousTpr: Double = 0
		var previousFpr: Double = 0
		i = pred.length - 1
		while (i >= 0) {
			//0 index is for threshold value
			if (pred(i) != null) {
				//accumulate auc
				auc = auc + previousTpr * (pred(i)(2) - previousFpr) + 0.5 * (pred(i)(2) - previousFpr) * (pred(i)(1) - previousTpr)
				//update previous
				previousTpr = pred(i)(1)
				previousFpr = pred(i)(2)
			}
			i = i - 1
		}
		auc
	}

	override def toString: String = {
		"RocObject: pred = %.4f\t".format(
			Arrays.toString(pred(0)))
	}
	/**
	 * Sum in-place to avoid new object alloc
	 */
	def addIn(other: RocObject): RocObject = {
		// Sum tpr, fpr for each threshold
		var i: Int = 0
		//start from 1, 0-index is for threshold value
		var j: Int = 1
		while (i < this.pred.length) {
			if (this.pred(i) != null) {
				if (other.pred(i) != null) {
					j = 1
					//P = P + P
					//N = N + N
					while (j < this.pred(i).length) {
						this.pred(i)(j) = this.pred(i)(j) + other.pred(i)(j)
						j = j + 1
					}
				}
			}
			else {
				if (other.pred(i) != null) {
					j = 0
					//P = P + P
					//N = N + N
					//this.pred(i) is currently null so need to cretae new instance
					this.pred(i) = new Array[Double](3)
					while (j < other.pred(i).length) {
						this.pred(i)(j) = other.pred(i)(j)
						j = j + 1
					}
				}
			}
			i = i + 1
		}
		this
	}
}

object Metrics extends TCanLog {
	val square = scala.math.pow(_: Double, 2)
	// Compute the R^2 score, aka http://en.wikipedia.org/wiki/Coefficient_of_determination
	def R2Score(yTrueYpred: RDD[(Double, Double)]): Double = {
		val meanYtrue = yTrueYpred.map({ case (ytrue, ypred) ⇒ ytrue }).mean()
		R2Score(yTrueYpred, meanYtrue)
	}

	// Compute the R^2 score given known yMean
	def R2Score(yTrueYpred: RDD[(Double, Double)], meanYtrue: Double): Double = {
		// collection of tuples of squared values of
		//   - difference from the mean
		//   - residuals
		val ss = yTrueYpred.map {
			case (ytrue, ypred) ⇒
				(square(ytrue - meanYtrue), square(ytrue - ypred))
		}
		// ss.collect().foreach(println(_))

		// sum of squares
		val sssum = ss.safeReduce((a, b) ⇒ (a._1 + b._1, a._2 + b._2), (Double.NaN, Double.NaN))
		val (sstot, ssres) = sssum
		if (sstot == 0) {
		  1
		} else {
		  1 - (ssres / sstot);
		}
	}

	def toByte(b: Boolean): Byte = if (b) 1 else 0
	/*
		 * Compute the confusion matrix for a binary classification model, given a threshold.
		 * The given model should be able to predict y such that 0 <= y <= 1.
		 */
	def binaryConfusionMatrix(yTrueYpred: RDD[(Double, Double)], threshold: Double) = {
		yTrueYpred.map {
			case (ytrue, ypred) ⇒
				// ground truth == positive?
				val isPos = toByte(ytrue > threshold)

				// predicted == positive?
				val predPos = toByte(ypred > threshold)

				val result = Array[Long](0, 0, 0, 0) // TN, FP, FN, TP, the logic is defined by next line
				result(isPos << 1 | predPos) = 1
				result
		}.reduce((a, b) ⇒ Array(a(0) + b(0), a(1) + b(1), a(2) + b(2), a(3) + b(3)))
	}

	/**
	 * LEGACY code, keep this here for future investigation if needed
	 * this is unoptimized version, I'm working on the optimized one.
	 * - return object using Array[Array[Double]], I would like to use Matrix instead
	 * - return object now kind of wasting memory (lot of null elements)
	 * - just drop my an email if you want to make major change
	 * - I need the println message for now, will remove later
	 * - Current algorithm require global-sorted prediction, that's why we need collect and sort here
	 * - Next will need to make it more mapreduce way by partial-sort, compute and somehow merge tpr, fpr
	 */
	/**
	 * Input data for computing ROC is the test data. Given that the data wouldn't be really 'big'.
	 * ROC is used for classification problem, so for now we will use Logistic regression to demonstrate.
	 * Given the input test data, we compute the prediction which is an array of Double
	 * By using the prediction array with the real label, we then output (threshold, true positive rate a.k.a tpr, false positive rate a.k.a fpr)
	 *
	 */
	def ROC_sequential(model: LogisticRegressionModel, XYData: RDD[(Matrix, Vector)]): RocObject = {
		val predictions = Predictions.yTrueYpred(model, XYData)
		var pred: Array[(Double, Double)] = predictions.collect()
		var previousVal: Double = Double.MaxValue
		var tp: Int = 0
		var fp: Int = 0
		var P: Int = 0
		var N: Int = 0
		//count number of positve, negative test instance
		var lpred: List[(Double, Double)] = pred.toList
		var c: Int = 0
		while (c < pred.size) {
			if (lpred(c)._1 == 1.0) P = P + 1
			c = c + 1
		}
		N = pred.size - P

		//sort by value score, INCREASING order. DO NOT change it for now 
		//algorithm: http://people.inf.elte.hu/kiss/13dwhdm/roc.pdf
		//time complexity: nlogn with n is the number of test instance

		var result: Array[Array[Double]] = new Array[Array[Double]](pred.size)
		var i: Int = 0
		lpred.sortBy(_._2) foreach {
			case (key, value) ⇒
				if (value != previousVal) {
					result(i) = new Array[Double](3)
					result(i)(0) = value
					result(i)(1) = tp / P.asInstanceOf[Double]
					result(i)(2) = fp / N.asInstanceOf[Double]
					previousVal = value
					i = i + 1
				}
				if (key == 1.0) {
					tp = tp + 1
				}
				else {
					fp = fp + 1
				}
		}
		//final: shoule be pushing equal to (1,1)
		result(i - 1)(0) = previousVal
		result(i - 1)(1) = tp / P.asInstanceOf[Double]
		result(i - 1)(2) = fp / N.asInstanceOf[Double]
		//build matrix later
		//val rocObject = new RocObject(Matrix.newInstance(result))
		new RocObject(result, 0.0)
	}

	/*
	 * ROC optimzed version
	 * convert prediction score to one specific range score in alpha array
	 * time complexity for mapping score: constant
	 */

	def ROC(XYData: RDD[Array[Array[Double]]], alpha_length: Int): RocObject = {
		var alpha: Array[Double] = new Array[Double](alpha_length)

    	
		//		XYData.mapPartitions(f, preservesPartitioning)
		var roc = XYData.map(mappingPredictToThreshold(alpha_length)).safeReduce(_.addIn(_))

		// TODO: check of roc is null
		var pred = roc.pred
		var previousVal: Double = Double.MaxValue
		var P: Double = 0.0
		var N: Double = 0.0

		//count number of positve, negative test instance
		var c: Int = 0
		// c= number of partition
		
		//check if pred.length == 0 or not
		if(pred.length > 0) {
			while (c < pred.length) {
				if (pred(c) != null) {
					P = P + pred(c)(1)
					N = N + pred(c)(2)
				}
				c = c + 1
			}
		}
		else {
			throw new IllegalArgumentException("Please try to run on binary classification model or contact system operators for assistance");
		}

		var result: Array[Array[Double]] = new Array[Array[Double]](pred.size)
		var i: Int = 0
		var count: Int = 0

		var tp: Double = 0.0
		var fp: Double = 0.0
		var accumulatetp: Double = 0.0
		var accumulatefp: Double = 0.0
		var accumulatefn: Double = 0.0
		var accumulatetn: Double = 0.0

		i = pred.length - 1
		var lastNotNullIndex: Int = 0
		//final result, 'merge' all intermediate result
		while (i >= 0) {
			//0 index is for threshold value
			if (pred(i) != null) {
				
				if(lastNotNullIndex == 0) lastNotNullIndex = i
				
				tp = pred(i)(1)
				fp = pred(i)(2)

				accumulatetp = accumulatetp + tp
				accumulatefp = accumulatefp + fp

				result(i) = new Array[Double](9)
				result(i)(0) = pred(i)(0)

				//true positive rate
				if (P != 0) {
					result(i)(1) = (accumulatetp / P).asInstanceOf[Double]
				}
				//P == 0 meaning, all accumulatetp = 0, therefore 0/0, let make it 0
				else {
					result(i)(1) = accumulatetp
				}
				//false positive rate
				if (N != 0) {
					result(i)(2) = (accumulatefp / N).asInstanceOf[Double]
				}
				else {
					result(i)(2) = accumulatefp
				}

				//precision
				result(i)(3) = (accumulatetp / (accumulatetp + accumulatefp)).asInstanceOf[Double]
				//recall is same as tpr
				result(i)(4) = result(i)(1)

				accumulatefn = P - accumulatetp
				accumulatetn = N - accumulatefp

				//sensitivity is the same as true positive rate
				result(i)(5) = result(i)(1)
				//specificity is the same as true negative rate
				result(i)(6) = (accumulatetn / (accumulatetn + accumulatefp)).asInstanceOf[Double]

				//f1 score
				result(i)(7) = (2 * accumulatetp / (2 * accumulatetp + accumulatefp + accumulatefn)).asInstanceOf[Double]
				//accuracy
				result(i)(8) = (accumulatetp + accumulatetn) / (accumulatetp + accumulatetn + accumulatefp + accumulatefn)
				count = count + 1
			}
			i = i - 1
		}

		//filter null/NA in pred
//		var result2: Array[Array[Double]] = new Array[Array[Double]](count)
		val result2: Array[Array[Double]] = new Array[Array[Double]](pred.length)
		i = 0
		var j: Int = 0
		var previousTpr: Double = 0
		var previousFpr: Double = 0
		//		var auc: Double = 0

		while (i < pred.length) {
			//0 index is for threshold value
			if (pred(i) != null) {
				result2(j) = result(i)
				var threshold: Double = i * 1 / alpha_length.asInstanceOf[Double]
				result2(j)(0) = threshold //pred(i)(0)
				//accumulate auc
				//update previous
				previousTpr = result2(j)(1)
				previousFpr = result2(j)(2)
				j = j + 1
			}
			//in case some threshold data points are null, we just automatically repeated the lastNonNull metrics
			//the purpose is to make the data much more comprehensive for users 
			else if (i >= lastNotNullIndex + 1) {
				result2(j) = new Array[Double](result(lastNotNullIndex).length)
				var t: Int = 1
				while(t < result(lastNotNullIndex).length) {
					result2(j)(t) = result(lastNotNullIndex)(t)
					t += 1
				}
				var threshold: Double = i * 1 / alpha_length.asInstanceOf[Double]
				result2(j)(0) = threshold //pred(i)(0)
				j = j + 1
			}
			i = i + 1
		}
		var ret: RocObject = new RocObject(result2, 0.0)
		ret.computeAUC

		ret
	}
	/*
	 * compute TP, FP for each prediction partition
	 * input: partition <Vector, Vector>
	 * output: Array: alpha threshold, tp, fp
	 * 
	 * !!! NOTE: the algorithm implicitly assume the yTrue is 1st column and predict is 2nd column
	 * 
	 * Array: length = alpha_length
	 * each element: threshold, positve_frequency, negative_frequency
	 */
	def mappingPredictToThreshold(alpha_length: Int)(input: Array[Array[Double]]): RocObject = {
		//loop thorugh all test instance
		var output: Array[Array[Double]] = new Array[Array[Double]](alpha_length)
		var predict = 0.0
		var yTrue = 0.0
		var index: Int = 0
		var threshold: Double = 0.0

		for (i ← 0 until input.length - 1) {

			predict = input(i)(1)
			yTrue = input(i)(0)
			
			//model.predict(Vector(input._1.getRow(i)))
			index = getAlpha(predict, alpha_length)
			threshold = getThreshold(predict, alpha_length)

			//update 
			if (output(index) != null) {
				output(index)(0) = threshold
				if (yTrue == 1.0) {
					//positve_frequency++
					output(index)(1) = output(index)(1) + 1.0
				}
				else {
					//negative_frequency++
					output(index)(2) = output(index)(2) + 1.0
				}
			} //create new element
			else {
				output(index) = new Array[Double](3)
				output(index)(0) = threshold
				if (yTrue == 1.0) {
					//positve_frequency++
					output(index)(1) = 1.0
				}
				else {
					//negative_frequency++
					output(index)(2) = 1.0
				}
			}
		}
		var roc = new RocObject(output, 0.0)
		roc
	}
	
	/*
	 * get threshold in alpha array
	 */
	def getAlpha(score: Double, alpha_length: Int): Int = {
		var index: Int = math.floor((score * alpha_length).asInstanceOf[Double]).asInstanceOf[Int]
		//re-indexing if score is precisely 1.0 to avoid ArrayOutofIndex exception
		if (index == alpha_length)
			index = index - 1
		index
	}

	/*
	 * 
	 */
	def getThreshold(predict: Double, alpha_length: Int): Double = {
		var index = getAlpha(predict, alpha_length)
		index * 1 / alpha_length.asInstanceOf[Double]
	}

	def AucScore(model: LogisticRegressionModel, XYData: RDD[(Matrix, Vector)]) {
		1.23345 // TODO(khang)
	}
}
