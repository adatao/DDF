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

package adatao.bigr.spark.types

import adatao.ML.ATestSuite
import com.google.gson.GsonBuilder
import adatao.ML.LogisticRegressionModel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class SerializationSuite extends ATestSuite {

	test("Deserialization of ExecutionResult[LogisticRegressionModel]] with 'resultType' works") {
		val json = "{'success':true,'result':{'weights':[-3.0251478542873445,1.4117674848394235,-0.9493392810289571]," +
			"'trainingLosses':[0.6490607068850122,0.6158131123188525], 'numSamples': 10},'persistenceID':'9d44cc4c-4919-4e12-b096-432fcf052962'," +
			"'resultType':'adatao.ML.LogisticRegressionModel'}"

		val result = ExecutionResult.fromJson[LogisticRegressionModel](json)

		assert(result != null)
		assert(result.result.getClass.equals(classOf[LogisticRegressionModel]))
	}

	test("Deserialization of ExecutionResult[LogisticRegressionModel]] WITHOUT 'resultType' works") {
		val json = "{'success':true,'result':{'weights':[-3.0251478542873445,1.4117674848394235,-0.9493392810289571]," +
			"'trainingLosses':[0.6490607068850122,0.6158131123188525], 'numSamples': 10},'persistenceID':'9d44cc4c-4919-4e12-b096-432fcf052962'," +
			"'WITHOUT_resultType':'adatao.ML.LogisticRegressionModel'}"

		val result = ExecutionResult.fromJson[LogisticRegressionModel](json)

		assert(result != null)
		assert(result.result.getClass.equals(classOf[LogisticRegressionModel]))
	}

}
