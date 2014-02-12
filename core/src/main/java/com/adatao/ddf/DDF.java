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
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * <p>
 * A Distributed DDF (DDF) has a number of key properties (metadata, representations, etc.) and
 * capabilities (self-compute basic statistics, aggregations, etc.).
 * </p>
 * <p>
 * </p>
 * 
 * @author ctn
 * 
 */
public class DDF implements ISupportPhantomReference {

  /**
   * Instantiates a new DDF with the given ADDFHelper
   * 
   * @param ADDFHelper
   */
  public DDF(ADDFHelper setHelper) {
    this.setHelper(setHelper);
    if (setHelper != null) setHelper.setDDF(this);

    PhantomReference.register(this);
  }


  private ADDFHelper mHelper;

  /**
   * @return the underlying ADDFHelper of this DDF
   */
  public ADDFHelper getHelper() {
    if (mHelper != null) return mHelper;
    else throw new UnsupportedOperationException("No implementor has been set");
  }

  /**
   * Sets the underlying implementor for this DDF
   * 
   * @param aADDFHelper
   */
  public void setHelper(ADDFHelper aHelper) {
    this.mHelper = aHelper;
  }


  public IComputeBasicStatistics getBasicStatisticsComputer() {
    return this.getHelper().getBasicStatisticsComputer();
  }



  public IHandleIndexing getIndexingHandler() {
    return this.getHelper().getIndexingHandler();
  }

  public IHandleJoins getJoinsHandler() {
    return this.getHelper().getJoinsHandler();
  }

  public IHandleMetaData getMetaDataHandler() {
    return this.getHelper().getMetaDataHandler();
  }


  public IHandleMiscellany getMiscellanyHandler() {
    return this.getHelper().getMiscellanyHandler();
  }

  public IHandleMissingData getMissingDataHandler() {
    return this.getHelper().getMissingDataHandler();
  }

  public IHandleMutability getMutabilityHandler() {
    return this.getHelper().getMutabilityHandler();
  }

  public IHandlePersistence getPersistenceHandler() {
    return this.getHelper().getPersistenceHandler();
  }

  public IHandleRepresentations getRepresentationHandler() {
    return this.getHelper().getRepresentationHandler();

  }

  public IHandleReshaping getReshapingHandler() {
    return this.getHelper().getReshapingHandler();
  }

  public IHandleSchema getSchemaHandler() {
    return this.getHelper().getSchemaHandler();
  }

  public IHandleStreamingData getStreamingDataHandler() {
    return this.getHelper().getStreamingDataHandler();
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    return this.getHelper().getTimeSeriesHandler();
  }


  public IHandleViews getViewHandler() {
    return this.getHelper().getViewHandler();
  }

  public IRunAlgorithms getAlgorithmRunner() {
    return this.getHelper().getAlgorithmRunner();
  }


  public long getNumRows(){
    return this.getMetaDataHandler().getNumRows();
  }
  
  public long getNumColumns(){
    return this.getMetaDataHandler().getNumColumns();
  }
    
  public Schema getSchema(){
    return this.getMetaDataHandler().getSchema();
  }


  /**
   * @param numSamples
   * @return a new DDF containing `numSamples` rows selected randomly from this DDF.
   */


  public DDF getRandomSample(int numSamples) {
    return this.getViewHandler().getRandomSample(numSamples);
  }

  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was
   * registered under {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    this.setHelper(null);
  }

  // /////////////////////////////////////
  // Content: Views & Representations
  // /////////////////////////////////////

  /**
   * Override to implement, e.g., in-memory caching support
   */
  public void cache() {
    // Nothing
  }

  /**
   * Override to implement, e.g., in-memory caching support
   */
  public void uncache() {
    // Nothing
  }


  // /////////////////////////////////////
  // ETL
  // /////////////////////////////////////


  // /////////////////////////////////////
  // Analytics
  // /////////////////////////////////////
}
