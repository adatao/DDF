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

package com.adatao.ddf.spark

import com.adatao.ddf.DDF
import org.apache.spark.rdd.RDD
import com.adatao.ddf.ADDFHelper
import com.adatao.ddf.IDDFFactory
import com.adatao.ddf.spark.content.RepresentationHandler

/**
 * <p>
 * An RDD-based representation and implementation of a DDF.
 * In particular DDFs are represented by underlying immutable RDDs.
 * This class allows users to access a number of useful, Spark-related
 * idioms and facilities, including:
 * </p>
 * <ul>
 * <li>The various underlying RDD representations of a DDF</li>
 * <li>Convenient conversions between different RDD types, e.g., row- or column-based</li>
 * <li>Easy access to Spark's MLLib algorithms</li>
 * <li>Shark's SQL facilities</li>
 * <li>Ability to start with an RDD and get a DDF, and vice versa.</li>
 * </ul>
 *
 * @author ctn
 */
class DDFHelper(ddf: DDF) extends ADDFHelper(ddf) {
	this
		.setAlgorithmRunner(null)
		.setBasicStatisticsHandler(null)
		.setFilterAndProjectionHandler(null)
		.setIndexingHandler(null)
		.setJoinsHandler(null)
		.setMetaDataHandler(null)
		.setMiscellanyHandler(null)
		.setMissingDataHandler(null)
		.setMutabilityHandler(null)
		.setPersistenceHandler(null)
		.setRepresentationHandler(new RepresentationHandler(this))
		.setReshapingHandler(null)
		.setSchemaHandler(null)
		.setStreamingDataHandler(null)
		.setTimeSeriesHandler(null)
}

object DDFHelper extends IDDFFactory {
	/**
	 * Instantiates a new DDF with all the Spark implementation
	 */
	def newDDF = new DDF(new DDFHelper(null))

	/**
	 * Instantiates a new DDF based on the supplied RDD[T]
	 */
	def newDDF[T](rdd: RDD[T])(implicit m: Manifest[T]): DDF = this.newDDF(rdd, m.erasure)

	/**
	 * Instantiates a new DDF based on the supplied RDD[_].
	 * This signature is useful for Java clients, due to type erasure, so the client
	 * needs to be able to pass in the elementType explicitly.
	 */
	def newDDF[T](rdd: RDD[T], elementType: Class[_]): DDF = {
		val ddf = newDDF
		ddf.getHelper().getRepresentationHandler().asInstanceOf[RepresentationHandler].set(rdd, classOf[RDD[T]], elementType)
		ddf
	}

}