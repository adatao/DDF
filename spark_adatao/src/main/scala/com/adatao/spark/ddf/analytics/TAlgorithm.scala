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

package com.adatao.spark.ddf.analytics

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.gson.Gson

/**
 * ATTN: com.adatao.ML must not depend on classes from execution-environment-specific packages.
 *
 * "Pure" algorithms belong to the package `com.adatao.pa.ML`. In particular, the
 * class hierarchy and interfaces are defined such that they are not bothered with
 * the requirements of the execution environment. To see how the particulars of
 * the execution environment (e.g, BigR over Spark) are handled, see the
 * corresponding classes in the package `com.adatao.pa.spark.execution`.
 *
 * Here's a class hierarchy overview. Any ML algorithm is comprised of two key
 * classes, the algorithm itself (TAlgorithm), and the trained model (TModel).
 * *
 * As an example, LinearRegression extends TAlgorithm. It also defines two
 * companion-object classes ("static classes") named Input and Output, corresponding
 * to the training parameters, and the model output, respectively.
 *
 * LinearRegressionModel extends TModel. It also defines two companion-object
 * classes named Input and Output, corresponding
 * to the prediction input features and output prediction, respectively.
 *
 * @author ctn
 */

/**
 * Objects exhibit this trait to get access to a LOG object
 */
trait TCanLog {
	protected var _LOG: Logger = null
	protected def LOG: Logger = {
		if (_LOG == null) _LOG = LoggerFactory.getLogger(this.getClass)
		_LOG
	}
}

trait TToJsonString {
	override def toString: String = new Gson().toJson(this)
}

/**
 * Base class for implementation of any algorithm's training methods
 */
trait TAlgorithm extends Serializable with TCanLog with TToJsonString {
}

/**
 * Base class to serve as common ancestor for all Models
 */
trait TModel extends Serializable with TCanLog with TToJsonString {
    def ddfModelID: String = { null }
}

/**
 * Base class for implementation of any algorithm's prediction methods
 */
trait TPredictiveModel[InputType, PredictionType] extends TModel {
   def predict(input: InputType): PredictionType
}
