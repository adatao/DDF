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
import com.adatao.ddf.content.IHandleMetaData;
import com.adatao.ddf.content.IHandleMissingData;
import com.adatao.ddf.content.IHandleMutability;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.IHandleSchema;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;
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
 * |    DDF    |<-->|       ADDFManager      |
 * -------------    -------------------------
 *                         ^          ^
 *                         |   ...    |        -------------------
 *                         |          |------->| IHandleMetadata |
 *                         |                   -------------------
 *                         |
 *                         |        ----------------------------------
 *                         |------->| IHandleRepresentations |
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
public abstract class ADDFManager implements IDDFManager, ISupportPhantomReference {

  public ADDFManager() {
  }

  public ADDFManager(DDF theDDF) {
    this.setDDF(theDDF);

    PhantomReference.register(this);
  }


  private DDF mDDF;

  /**
   * 
   * @return the DDF that I directly manage, if any
   */
  public DDF getDDF() {
    return mDDF;
  }

  /**
   * Sets the DDF that I should directly manage
   * 
   * @param aDDF
   * @return
   */
  public ADDFManager setDDF(DDF aDDF) {
    this.mDDF = aDDF;
    return this;
  }


  private IComputeBasicStatistics mBasicStatisticsComputer;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetaData mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandlePersistence mPersistenceHandler;
  private IHandleRepresentations mRepresentationHandler;
  private IHandleReshaping mReshapingHandler;
  private IHandleSchema mSchemaHandler;
  private IHandleStreamingData mStreamingDataHandler;
  private IHandleTimeSeries mTimeSeriesHandler;
  private IHandleViews mViewHandler;
  private IRunAlgorithms mAlgorithmRunner;


  public IComputeBasicStatistics getBasicStatisticsComputer() {
    if (mBasicStatisticsComputer == null) mBasicStatisticsComputer = this.createBasicStatisticsComputer();
    if (mBasicStatisticsComputer == null) throw new UnsupportedOperationException();
    else return mBasicStatisticsComputer;
  }

  public ADDFManager setBasicStatisticsComputer(IComputeBasicStatistics aBasicStatisticsComputer) {
    this.mBasicStatisticsComputer = aBasicStatisticsComputer;
    return this;
  }

  protected abstract IComputeBasicStatistics createBasicStatisticsComputer();


  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) mIndexingHandler = this.createIndexingHandler();
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public ADDFManager setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
    return this;
  }

  protected abstract IHandleIndexing createIndexingHandler();


  public IHandleJoins getJoinsHandler() {
    if (mJoinsHandler == null) mJoinsHandler = this.createJoinsHandler();
    if (mJoinsHandler == null) throw new UnsupportedOperationException();
    else return mJoinsHandler;
  }

  public ADDFManager setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
    return this;
  }

  protected abstract IHandleJoins createJoinsHandler();


  public IHandleMetaData getMetaDataHandler() {
    if (mMetaDataHandler == null) mMetaDataHandler = this.createMetaDataHandler();
    if (mMetaDataHandler == null) throw new UnsupportedOperationException();
    else return mMetaDataHandler;
  }

  public ADDFManager setMetaDataHandler(IHandleMetaData aMetaDataHandler) {
    this.mMetaDataHandler = aMetaDataHandler;
    return this;
  }

  protected abstract IHandleMetaData createMetaDataHandler();


  public IHandleMiscellany getMiscellanyHandler() {
    if (mMiscellanyHandler == null) mMiscellanyHandler = this.createMiscellanyHandler();
    if (mMiscellanyHandler == null) throw new UnsupportedOperationException();
    else return mMiscellanyHandler;
  }

  public ADDFManager setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
    return this;
  }

  protected abstract IHandleMiscellany createMiscellanyHandler();


  public IHandleMissingData getMissingDataHandler() {
    if (mMissingDataHandler == null) mMissingDataHandler = this.createMissingDataHandler();
    if (mMissingDataHandler == null) throw new UnsupportedOperationException();
    else return mMissingDataHandler;
  }

  public ADDFManager setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
    return this;
  }

  protected abstract IHandleMissingData createMissingDataHandler();


  public IHandleMutability getMutabilityHandler() {
    if (mMutabilityHandler == null) mMutabilityHandler = this.createMutabilityHandler();
    if (mMutabilityHandler == null) throw new UnsupportedOperationException();
    else return mMutabilityHandler;
  }

  public ADDFManager setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
    return this;
  }

  protected abstract IHandleMutability createMutabilityHandler();


  public IHandlePersistence getPersistenceHandler() {
    if (mPersistenceHandler == null) mPersistenceHandler = this.createPersistenceHandler();
    if (mPersistenceHandler == null) throw new UnsupportedOperationException();
    else return mPersistenceHandler;
  }

  public ADDFManager setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
    return this;
  }

  protected abstract IHandlePersistence createPersistenceHandler();


  public IHandleRepresentations getRepresentationHandler() {
    if (mRepresentationHandler == null) mRepresentationHandler = this.createRepresentationHandler();
    if (mRepresentationHandler == null) throw new UnsupportedOperationException();
    else return mRepresentationHandler;
  }

  public ADDFManager setRepresentationHandler(IHandleRepresentations aRepresentationHandler) {
    this.mRepresentationHandler = aRepresentationHandler;
    return this;
  }

  protected abstract IHandleRepresentations createRepresentationHandler();


  public IHandleReshaping getReshapingHandler() {
    if (mReshapingHandler == null) mReshapingHandler = this.createReshapingHandler();
    if (mReshapingHandler == null) throw new UnsupportedOperationException();
    else return mReshapingHandler;
  }

  public ADDFManager setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
    return this;
  }

  protected abstract IHandleReshaping createReshapingHandler();


  public IHandleSchema getSchemaHandler() {
    if (mSchemaHandler == null) mSchemaHandler = this.createSchemaHandler();
    if (mSchemaHandler == null) throw new UnsupportedOperationException();
    else return mSchemaHandler;
  }

  public ADDFManager setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
    return this;
  }

  protected abstract IHandleSchema createSchemaHandler();


  public IHandleStreamingData getStreamingDataHandler() {
    if (mStreamingDataHandler == null) mStreamingDataHandler = this.createStreamingDataHandler();
    if (mStreamingDataHandler == null) throw new UnsupportedOperationException();
    else return mStreamingDataHandler;
  }

  public ADDFManager setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
    return this;
  }

  protected abstract IHandleStreamingData createStreamingDataHandler();


  public IHandleTimeSeries getTimeSeriesHandler() {
    if (mTimeSeriesHandler == null) mTimeSeriesHandler = this.createTimeSeriesHandler();
    if (mTimeSeriesHandler == null) throw new UnsupportedOperationException();
    else return mTimeSeriesHandler;
  }

  public ADDFManager setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
    return this;
  }

  protected abstract IHandleTimeSeries createTimeSeriesHandler();


  public IHandleViews getViewHandler() {
    if (mViewHandler == null) mViewHandler = this.createViewHandler();
    if (mViewHandler == null) throw new UnsupportedOperationException();
    else return mViewHandler;
  }

  public ADDFManager setViewHandler(IHandleViews aViewHandler) {
    this.mViewHandler = aViewHandler;
    return this;
  }

  protected abstract IHandleViews createViewHandler();


  public IRunAlgorithms getAlgorithmRunner() {
    if (mAlgorithmRunner == null) mAlgorithmRunner = this.createAlgorithmRunner();
    if (mAlgorithmRunner == null) throw new UnsupportedOperationException();
    else return mAlgorithmRunner;
  }

  public ADDFManager setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
    return this;
  }

  protected abstract IRunAlgorithms createAlgorithmRunner();


  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was
   * registered under {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    // @formatter:off
    this
    .setDDF(null)
    .setAlgorithmRunner(null)
    .setBasicStatisticsComputer(null)
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
    .shutdown()
    ;
    // @formatter:on
  }
  
  public abstract DDF load(String command, Schema schema) throws DDFException;
  
  public abstract DDF load(String command, DataFormat dataFormat) throws DDFException;
  
  public abstract DDF load(String command, Schema schema, String dataSource) throws DDFException;
  
  public abstract DDF load(String command, Schema schema, DataFormat dataFormat) throws DDFException;
  
  public abstract DDF load(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException;
  
  public abstract void runSqlCommand(String command) throws DDFException;
}
