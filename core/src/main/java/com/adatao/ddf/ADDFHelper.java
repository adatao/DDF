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
package com.adatao.ddf;

import com.adatao.ddf.analytics.IComputeBasicStatistics;
import com.adatao.ddf.analytics.IRunAlgorithms;
import com.adatao.ddf.content.IHandleIndexing;
import com.adatao.ddf.content.IHandleMetadata;
import com.adatao.ddf.content.IHandleMissingData;
import com.adatao.ddf.content.IHandleMutability;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.IHandleSchema;
import com.adatao.ddf.etl.IHandleFilteringAndProjections;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * <p>
 * Abstract base class for a {@link DDF} implementor, which provides the support methods necessary
 * to implement various DDF interfaces, such as {@link IHandleRepresentations} and
 * {@link IRunAlgorithms}.
 * </p>
 * <p>
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to
 * provide alternative (even snap-in replacements), support/implementation for DDF. The class
 * diagram is as follows:
 * </p>
 * 
 * <pre>
 * -------------    -------------------------
 * |    DDF    |<-->|       ADDFHelper      |
 * -------------    -------------------------
 *                         ^          ^
 *                         |   ...    |        -------------------
 *                         |          |------->| IHandleMetadata |
 *                         |                   -------------------
 *                         |
 *                         |        ----------------------------------
 *                         |------->| IHandleFilteringAndProjections |
 *                                  ----------------------------------
 * </pre>
 * <p>
 * An implementor need not provide all or even most of these interfaces. Each interface handler can
 * be get/set separately, as long as they cooperate properly on things like the underlying
 * representation. This makes it easy to roll out additional interfaces and their implementations
 * over time.
 * </p>
 * 
 * @author ctn
 * 
 */
public abstract class ADDFHelper implements IDDFHelper, ISupportPhantomReference {

  public ADDFHelper(DDF theDDF) {
    this.setDDF(theDDF);
    
    PhantomReference.register(this);
  }


  private DDF mDDF;

  public DDF getDDF() {
    return mDDF;
  }

  public ADDFHelper setDDF(DDF aDDF) {
    this.mDDF = aDDF;
    return this;
  }


  private IComputeBasicStatistics mBasicStatisticsHandler;
  private IHandleFilteringAndProjections mFilterAndProjectionHandler;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetadata mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandlePersistence mPersistenceHandler;
  private IHandleRepresentations mRepresentationHandler;
  private IHandleReshaping mReshapingHandler;
  private IHandleSchema mSchemaHandler;
  private IHandleStreamingData mStreamingDataHandler;
  private IHandleTimeSeries mTimeSeriesHandler;
  private IRunAlgorithms mAlgorithmRunner;

  public IComputeBasicStatistics getBasicStatisticsHandler() {
    if (mBasicStatisticsHandler == null) throw new UnsupportedOperationException();
    else return mBasicStatisticsHandler;
  }

  public ADDFHelper setBasicStatisticsHandler(IComputeBasicStatistics aBasicStatisticsHandler) {
    this.mBasicStatisticsHandler = aBasicStatisticsHandler;
    return this;
  }

  public IHandleFilteringAndProjections getFilterAndProjectionHandler() {
    if (mFilterAndProjectionHandler == null) throw new UnsupportedOperationException();
    else return mFilterAndProjectionHandler;
  }

  public ADDFHelper setFilterAndProjectionHandler(IHandleFilteringAndProjections aFilterAndProjectionHandler) {
    this.mFilterAndProjectionHandler = aFilterAndProjectionHandler;
    return this;
  }

  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public ADDFHelper setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
    return this;
  }

  public IHandleJoins getJoinsHandler() {
    if (mJoinsHandler == null) throw new UnsupportedOperationException();
    else return mJoinsHandler;
  }

  public ADDFHelper setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
    return this;
  }

  public IHandleMetadata getMetaDataHandler() {
    if (mMetaDataHandler == null) throw new UnsupportedOperationException();
    else return mMetaDataHandler;
  }

  public ADDFHelper setMetaDataHandler(IHandleMetadata aMetaDataHandler) {
    this.mMetaDataHandler = aMetaDataHandler;
    return this;
  }

  public IHandleMiscellany getMiscellanyHandler() {
    if (mMiscellanyHandler == null) throw new UnsupportedOperationException();
    else return mMiscellanyHandler;
  }

  public ADDFHelper setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
    return this;
  }

  public IHandleMissingData getMissingDataHandler() {
    if (mMissingDataHandler == null) throw new UnsupportedOperationException();
    else return mMissingDataHandler;
  }

  public ADDFHelper setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
    return this;
  }

  public IHandleMutability getMutabilityHandler() {
    if (mMutabilityHandler == null) throw new UnsupportedOperationException();
    else return mMutabilityHandler;
  }

  public ADDFHelper setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
    return this;
  }

  public IHandlePersistence getPersistenceHandler() {
    if (mPersistenceHandler == null) throw new UnsupportedOperationException();
    else return mPersistenceHandler;
  }

  public ADDFHelper setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
    return this;
  }

  public IHandleRepresentations getRepresentationHandler() {
    if (mRepresentationHandler == null) throw new UnsupportedOperationException();
    else return mRepresentationHandler;
  }

  public ADDFHelper setRepresentationHandler(IHandleRepresentations aRepresentationHandler) {
    this.mRepresentationHandler = aRepresentationHandler;
    return this;
  }

  public IHandleReshaping getReshapingHandler() {
    if (mReshapingHandler == null) throw new UnsupportedOperationException();
    else return mReshapingHandler;
  }

  public ADDFHelper setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
    return this;
  }

  public IHandleSchema getSchemaHandler() {
    if (mSchemaHandler == null) throw new UnsupportedOperationException();
    else return mSchemaHandler;
  }

  public ADDFHelper setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
    return this;
  }

  public IHandleStreamingData getStreamingDataHandler() {
    if (mStreamingDataHandler == null) throw new UnsupportedOperationException();
    else return mStreamingDataHandler;
  }

  public ADDFHelper setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
    return this;
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    if (mTimeSeriesHandler == null) throw new UnsupportedOperationException();
    else return mTimeSeriesHandler;
  }

  public ADDFHelper setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
    return this;
  }

  public IRunAlgorithms getAlgorithmRunner() {
    if (mAlgorithmRunner == null) throw new UnsupportedOperationException();
    else return mAlgorithmRunner;
  }

  public ADDFHelper setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
    return this;
  }

  @Override
  // ISupportPhantomReference
  public void cleanup() {
    // @formatter:off
    this
    .setDDF(null)
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
    .setRepresentationHandler(null)
    .setReshapingHandler(null)
    .setSchemaHandler(null)
    .setStreamingDataHandler(null)
    .setTimeSeriesHandler(null)
    ;
    // @formatter:on
  }
}
