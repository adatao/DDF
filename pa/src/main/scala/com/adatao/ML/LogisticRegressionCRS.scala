package com.adatao.ML


import java.lang.String
import java.util.Arrays
import org.jblas.DoubleMatrix
import scala.util.Random
import org.jblas.MatrixFunctions
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import java.util.HashMap
import com.adatao.ddf.scalatypes.MatrixSparse


object LogisticRegressionCRS {

	/**
	 * This is the signature to be used by clients that can represent their data using [[Matrix]] and [[Vector]]
	 */
	def train(
		XYData: (Matrix, Vector),
		numIters: Int,
		learningRate: Double,
		ridgeLambda: Double,
		initialWeights: Vector,
		numFeatures: Int): LogisticRegressionModel = {

		this.train(new LogisticRegression.LossFunction(XYData, ridgeLambda), numIters, learningRate, initialWeights, numFeatures)
	}

	/**
	 * This is the signature to be used by clients wishing to inject their own loss function that can handle their own
	 * data representation (e.g., [[spark.RDD]]).
	 */
	def train[XYDataType](
		lossFunction: ALogisticGradientLossFunction[XYDataType],
		numIters: Int,
		learningRate: Double,
		initialWeights: Vector,
		numFeatures: Int)(implicit m: Manifest[XYDataType]): LogisticRegressionModel = {

		val (weights, trainingLosses, numSamples) = Regression.train(lossFunction, numIters, learningRate, initialWeights, numFeatures)
		new LogisticRegressionModel(weights, trainingLosses, numSamples)
	}

}