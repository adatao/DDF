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

package com.adatao.ML

import org.jblas.DoubleMatrix
import java.util.HashMap
import com.adatao.ddf.types._

/**
 * Constructor parameters are accessible via 'val' so they would show up on (JSON) serialization
 */

abstract class ALinearModel[OutputType](val weights: Vector, val numSamples: Long) extends TPredictiveModel[Vector, OutputType] {
    //dummy mapping columng used in lm/glm
	var dummyColumnMapping = new HashMap[java.lang.Integer, HashMap[String, java.lang.Double]] () 
	var mapReferenceLevel = new HashMap[String, String] ()
	def predict(features: Vector): OutputType
	/**
	 * Helpful signature for caller to be able to pass in a DoubleMatrix directly
	 */
	def predict(features: DoubleMatrix): OutputType = this.predict(Vector(features))

	/**
	 * Helpful signature for caller to be able to pass in an Array[Double] directly.
	 * A Vector is built from the Array with a bias term.
	 */
	def predict(features: Array[Double]): OutputType = {
	  
	  println(">>>>>>>>>>>>>>>>>>>>>>> calling predict inside ALinearModel")
	  this.predict(Vector(Array[Double](1) ++ features))
	}

	protected def linearPredictor(features: Vector): Double = {
		weights.dot(features)
	}
}
object ALinearModel {
	val MAXNUMFEATURES_DEFAULT = 50
}
abstract class AIterativeLinearModel[OutputType](weights: Vector, val trainingLosses: Vector, numSamples: Long) extends ALinearModel[OutputType](weights, numSamples) {
  
}

/**
 * [[AContinuousLinearModel]] is a continuous-valued output predictive model, taking as input
 * a features [[Vector]].
 *
 * @param weights - The weights assigned to each feature (including the bias term)
 * @param trainingLosses - History of errors/losses during training. Not relevant to prediction, but available as part of training output.
 */

abstract class AContinuousIterativeLinearModel(weights: Vector, trainingLosses: Vector, numSamples: Long)
		extends AIterativeLinearModel[Double](weights, trainingLosses, numSamples) {
	override def predict(features: Vector): Double = this.linearPredictor(features)
}

/**
 * [[ADiscreteLinearModel]] is a continuous-valued output predictive model, taking as input
 * a features [[Vector]].
 *
 * @param weights - The weights assigned to each feature (including the bias term)
 * @param trainingLosses - History of errors/losses during training. Not relevant to prediction, but available as part of training output.
 */

abstract class ADiscreteIterativeLinearModel(weights: Vector, trainingLosses: Vector, numSamples: Long) extends AIterativeLinearModel[Int](weights, trainingLosses, numSamples) {
	override def predict(features: Vector): Int = if (this.linearPredictor(features) < 0.5) 0 else 1
}
