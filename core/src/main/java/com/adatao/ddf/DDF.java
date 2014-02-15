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

import java.util.List;

import com.adatao.ddf.analytics.IComputeBasicStatistics;
import com.adatao.ddf.analytics.IRunAlgorithms;
import com.adatao.ddf.content.IHandleIndexing;
import com.adatao.ddf.content.IHandleMetaData;
import com.adatao.ddf.content.IHandleMissingData;
import com.adatao.ddf.content.IHandleMutability;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.IHandleSchema;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;


/**
 * <p>
 * A Distributed DDF (DDF) has a number of key properties (metadata, representations, etc.) and
 * capabilities (self-compute basic statistics, aggregations, etc.).
 * </p>
 * <p>
 * This class was designed using the Bridge Pattern to provide clean separation between the abstract
 * concepts and the implementation so that the API can support multiple big data platforms under the
 * same set of abstract concepts.
 * </p>
 * 
 * @author ctn
 * 
 */
public class DDF {
  /**
   * Executes the given sqlCommand against this DDF, and returns the resulting DDF
   * 
   * @param sqlCommand
   * @return
   */
  public DDF sql2ddf(String sqlCommand) {
    return null;
  }

  /**
   * Executes the given sqlCommand against this DDF, and returns the resulting text result
   * 
   * @param sqlCommand
   * @return
   */
  public List<String> sql2txt(String sqlCommand) {
    return null;
  }

  private ADDFManager mManager;

  public ADDFManager getManager() {
    return this.mManager;
  }

  protected void setManager(ADDFManager aDDFManager) {
    this.mManager = aDDFManager;
  }


  public IComputeBasicStatistics getBasicStatisticsComputer() {
    return this.getManager().getBasicStatisticsComputer();
  }

  public IHandleIndexing getIndexingHandler() {
    return this.getManager().getIndexingHandler();
  }

  public IHandleJoins getJoinsHandler() {
    return this.getManager().getJoinsHandler();
  }

  public IHandleMetaData getMetaDataHandler() {
    return this.getManager().getMetaDataHandler();
  }

  public IHandleMiscellany getMiscellanyHandler() {
    return this.getManager().getMiscellanyHandler();
  }

  public IHandleMissingData getMissingDataHandler() {
    return this.getManager().getMissingDataHandler();
  }

  public IHandleMutability getMutabilityHandler() {
    return this.getManager().getMutabilityHandler();
  }

  public IHandlePersistence getPersistenceHandler() {
    return this.getManager().getPersistenceHandler();
  }

  public IHandleRepresentations getRepresentationHandler() {
    return this.getManager().getRepresentationHandler();
  }

  public IHandleReshaping getReshapingHandler() {
    return this.getManager().getReshapingHandler();
  }

  public IHandleSchema getSchemaHandler() {
    return this.getManager().getSchemaHandler();
  }

  public IHandleStreamingData getStreamingDataHandler() {
    return this.getManager().getStreamingDataHandler();
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    return this.getManager().getTimeSeriesHandler();
  }

  public IHandleViews getViewHandler() {
    return this.getManager().getViewHandler();
  }

  public IRunAlgorithms getAlgorithmRunner() {
    return this.getManager().getAlgorithmRunner();
  }

}
