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
package com.adatao.DDF;

/**
 * A DataFrame implementor provides the support methods necessary to implement various DataFrame
 * interfaces, such as {@link IHandleRepresentations} and {@link IRunAlgorithms}.
 * 
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to
 * provide alternative (even snap-in replacements), support/implementation for DDF.
 * 
 * An implementor need not provide all or even most of these interfaces. Each interface handler can
 * be get/set separately, as long as they cooperate properly on things like the underlying
 * representation. This makes it easy to roll out additional interfaces and their implementations
 * over time.
 * 
 * @author ctn
 * 
 */
public abstract class ADataFrameImplementor implements IDataFrameImplementor {

  private IComputeBasicStatistics mBasicStatisticsHandler;
  private IHandleFilteringAndProjections mFilterAndProjectionHandler;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetadata mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandlePersistence mPersistenceHandler;
  private IHandleReshaping mReshapingHandler;
  private IHandleSchema mSchemaHandler;
  private IHandleStreamingData mStreamingDataHandler;
  private IHandleTimeSeries mTimeSeriesHandler;
  private IPerformETL mETLPerformer;
  private IRunAlgorithms mAlgorithmRunner;

  public IComputeBasicStatistics getBasicStatisticsHandler() {
    if (mBasicStatisticsHandler == null) throw new UnsupportedOperationException();
    else return mBasicStatisticsHandler;
  }

  public void setBasicStatisticsHandler(IComputeBasicStatistics aBasicStatisticsHandler) {
    this.mBasicStatisticsHandler = aBasicStatisticsHandler;
  }

  public IHandleFilteringAndProjections getFilterAndProjectionHandler() {
    if (mFilterAndProjectionHandler == null) throw new UnsupportedOperationException();
    else return mFilterAndProjectionHandler;
  }

  public void setFilterAndProjectionHandler(IHandleFilteringAndProjections aFilterAndProjectionHandler) {
    this.mFilterAndProjectionHandler = aFilterAndProjectionHandler;
  }

  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public void setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
  }

  public IHandleJoins getJoinsHandler() {
    if (mJoinsHandler == null) throw new UnsupportedOperationException();
    else return mJoinsHandler;
  }

  public void setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
  }

  public IHandleMetadata getMetaDataHandler() {
    if (mMetaDataHandler == null) throw new UnsupportedOperationException();
    else return mMetaDataHandler;
  }

  public void setMetaDataHandler(IHandleMetadata aMetaDataHandler) {
    this.mMetaDataHandler = aMetaDataHandler;
  }

  public IHandleMiscellany getMiscellanyHandler() {
    if (mMiscellanyHandler == null) throw new UnsupportedOperationException();
    else return mMiscellanyHandler;
  }

  public void setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
  }

  public IHandleMissingData getMissingDataHandler() {
    if (mMissingDataHandler == null) throw new UnsupportedOperationException();
    else return mMissingDataHandler;
  }

  public void setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
  }

  public IHandleMutability getMutabilityHandler() {
    if (mMutabilityHandler == null) throw new UnsupportedOperationException();
    else return mMutabilityHandler;
  }

  public void setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
  }

  public IHandlePersistence getPersistenceHandler() {
    if (mPersistenceHandler == null) throw new UnsupportedOperationException();
    else return mPersistenceHandler;
  }

  public void setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
  }

  public IHandleReshaping getReshapingHandler() {
    if (mReshapingHandler == null) throw new UnsupportedOperationException();
    else return mReshapingHandler;
  }

  public void setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
  }

  public IHandleSchema getSchemaHandler() {
    if (mSchemaHandler == null) throw new UnsupportedOperationException();
    else return mSchemaHandler;
  }

  public void setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
  }

  public IHandleStreamingData getStreamingDataHandler() {
    if (mStreamingDataHandler == null) throw new UnsupportedOperationException();
    else return mStreamingDataHandler;
  }

  public void setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    if (mTimeSeriesHandler == null) throw new UnsupportedOperationException();
    else return mTimeSeriesHandler;
  }

  public void setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
  }

  public IPerformETL getETLPerformer() {
    if (mETLPerformer == null) throw new UnsupportedOperationException();
    else return mETLPerformer;
  }

  public void setETLPerformer(IPerformETL aETLPerformer) {
    this.mETLPerformer = aETLPerformer;
  }

  public IRunAlgorithms getAlgorithmRunner() {
    if (mAlgorithmRunner == null) throw new UnsupportedOperationException();
    else return mAlgorithmRunner;
  }

  public void setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
  }
}
