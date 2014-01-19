/**
 * Copyright 2014 Adatao, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.adatao.DDF.spark

import com.adatao.DDF.DataFrame
import org.apache.spark.rdd.RDD
import com.adatao.DDF.ADataFrameImplementor
import com.adatao.DDF.IDataFrameFactory

/**
 * @author ctn
 *
 * A Spark-based representation and implementation of a DDF.
 * In particular DDFs are represented by underlying immutable RDDs.
 * This class allows users to access a number of useful, Spark-related
 * idioms and facilities, including:
 * <ul>
 * <li>The various underlying RDD representations of a DDF</li>
 * <li>Convenient conversions between different RDD types, e.g., row- or column-based</li>
 * <li>Easy access to Spark's MLLib algorithms</li>
 * <li>Shark's SQL facilities</li>
 * <li>Ability to start with an RDD and get a DDF, and vice versa.</li>
 * </ul>
 */
class DataFrameImplementor extends ADataFrameImplementor {
}

object DataFrameImplementor extends IDataFrameFactory {
	/**
	 * Instantiates a new DataFrame with all the Spark implementation
	 */
	def newDataFrame: DataFrame = { new DataFrame }

	/**
	 * Instantiates a new DataFrame based on the supplied RDD[Any]
	 */
	def newDataFrame(rdd: RDD[Any]): DataFrame = { new DataFrame }
}