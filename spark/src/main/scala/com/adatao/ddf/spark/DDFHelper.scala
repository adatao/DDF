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
import com.adatao.ddf.analytics.IRunAlgorithms
import com.adatao.ddf.analytics.IComputeBasicStatistics
import com.adatao.ddf.etl.IHandleFilteringAndProjections
import com.adatao.ddf.content.IHandleIndexing
import com.adatao.ddf.etl.IHandleJoins
import com.adatao.ddf.content.IHandleMetadata
import com.adatao.ddf.IHandleMiscellany
import com.adatao.ddf.content.IHandleMissingData
import com.adatao.ddf.content.IHandleMutability
import com.adatao.ddf.etl.IHandlePersistence
import com.adatao.ddf.content.IHandleRepresentations
import com.adatao.ddf.content.IHandleSchema
import com.adatao.ddf.IHandleStreamingData
import com.adatao.ddf.IHandleTimeSeries
import com.adatao.ddf.spark.content.RepresentationHandler
import com.adatao.ddf.etl.IHandleReshaping
import com.adatao.ddf.content.IHandleViews
import com.adatao.ddf.spark.content.ViewHandler
import com.adatao.ddf.etl.PersistenceHandler

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

	//
	// These create* methods will be lazily called as needed, so we don't
	// instantiate more handler objects that necessary.
	//
	override protected def createAlgorithmRunner: IRunAlgorithms = null
	override protected def createBasicStatisticsComputer: IComputeBasicStatistics = null
	override protected def createFilteringAndProjectionsHandler: IHandleFilteringAndProjections = null
	override protected def createIndexingHandler: IHandleIndexing = null
	override protected def createJoinsHandler: IHandleJoins = null
	override protected def createMetadataHandler: IHandleMetadata = null
	override protected def createMiscellanyHandler: IHandleMiscellany = null
	override protected def createMissingDataHandler: IHandleMissingData = null
	override protected def createMutabilityHandler: IHandleMutability = null
	override protected def createPersistenceHandler: IHandlePersistence = new PersistenceHandler(this)
	override protected def createRepresentationHandler: IHandleRepresentations = new RepresentationHandler(this)
	override protected def createReshapingHandler: IHandleReshaping = null
	override protected def createSchemaHandler: IHandleSchema = null
	override protected def createStreamingDataHandler: IHandleStreamingData = null
  override protected def createTimeSeriesHandler: IHandleTimeSeries = null
  override protected def createViewHandler: IHandleViews = new ViewHandler(this)

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
		ddf.getHelper().getRepresentationHandler().asInstanceOf[RepresentationHandler].set(rdd, elementType)
		ddf
	}

}