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
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to provide alternative
 * (even snap-in replacements), support/implementation for DDF.
 * 
 * A DDF has a number of key properties (metadata, representations, etc.) and capabilities (self-compute basic
 * statistics, aggregations, etc.).
 * 
 * @author ctn
 * 
 */
public class DataFrame implements IComputeBasicStatistics, IHandleFilteringAndProjections, IHandleIndexing,
    IHandleJoins, IHandleMiscellany, IHandleMissingData, IHandleMutability, IHandlePersistence, IHandleReshaping,
    IHandleSchema, IHandleStreamingData, IHandleTimeSeries, IPerformETL, IRunAlgorithms {

  private IComputeBasicStatistics mBasicStatisticsHandler;
  private IHandleFilteringAndProjections mFilterAndProjectionHandler;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
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
    return mBasicStatisticsHandler;
  }

  public void setBasicStatisticsHandler(IComputeBasicStatistics aBasicStatisticsHandler) {
    this.mBasicStatisticsHandler = aBasicStatisticsHandler;
  }

  public IHandleFilteringAndProjections getFilterAndProjectionHandler() {
    return mFilterAndProjectionHandler;
  }

  public void setFilterAndProjectionHandler(IHandleFilteringAndProjections aFilterAndProjectionHandler) {
    this.mFilterAndProjectionHandler = aFilterAndProjectionHandler;
  }

  public IHandleIndexing getIndexingHandler() {
    return mIndexingHandler;
  }

  public void setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
  }

  public IHandleJoins getJoinsHandler() {
    return mJoinsHandler;
  }

  public void setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
  }

  public IHandleMiscellany getMiscellanyHandler() {
    return mMiscellanyHandler;
  }

  public void setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
  }

  public DataFrame getRandomSample(int numSamples) {
    return mMiscellanyHandler == null ? null : mMiscellanyHandler.getRandomSample(numSamples);
  }

  public IHandleMissingData getMissingDataHandler() {
    return mMissingDataHandler;
  }

  public void setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
  }

  public IHandleMutability getMutabilityHandler() {
    return mMutabilityHandler;
  }

  public void setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
  }

  public IHandlePersistence getPersistenceHandler() {
    return mPersistenceHandler;
  }

  public void setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
  }

  public IHandleReshaping getReshapingHandler() {
    return mReshapingHandler;
  }

  public void setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
  }

  public IHandleSchema getSchemaHandler() {
    return mSchemaHandler;
  }

  public void setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
  }

  public IHandleStreamingData getStreamingDataHandler() {
    return mStreamingDataHandler;
  }

  public void setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    return mTimeSeriesHandler;
  }

  public void setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
  }

  public IPerformETL getETLPerformer() {
    return mETLPerformer;
  }

  public void setETLPerformer(IPerformETL aETLPerformer) {
    this.mETLPerformer = aETLPerformer;
  }

  public IRunAlgorithms getAlgorithmRunner() {
    return mAlgorithmRunner;
  }

  public void setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
  }
}
