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
import com.adatao.ddf.content.*;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;

import java.util.List;


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

  private ADDFManager mDDFManager;

  public ADDFManager getDDFManager() {

    return mDDFManager;
  }

  public void setDDFManager(ADDFManager aDDFManager) {

    mDDFManager= aDDFManager;
  }
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

  public IComputeBasicStatistics getBasicStatisticsComputer() {

    return mDDFManager.getBasicStatisticsComputer();
  }

  public IHandleIndexing getIndexingHandler() {

    return mDDFManager.getIndexingHandler();
  }

  public IHandleJoins getJoinsHandler() {

    return mDDFManager.getJoinsHandler();
  }

  public IHandleMetaData getMetadataHandler() {

    return mDDFManager.getMetaDataHandler();
  }

  public IHandleMiscellany getMiscellanyHandler() {

    return mDDFManager.getMiscellanyHandler();
  }

  public IHandleMissingData getMissingDataHandler() {

    return mDDFManager.getMissingDataHandler();
  }

  public IHandleMutability getMutabilityHandler() {

    return mDDFManager.getMutabilityHandler();
  }

  public IHandlePersistence getPersistenceHandler() {

    return mDDFManager.getPersistenceHandler();
  }

  public IHandleRepresentations getRepresentationHandler() {

    return mDDFManager.getRepresentationHandler();
  }

  public IHandleReshaping getReshapingHandler() {

    return mDDFManager.getReshapingHandler();
  }

  public IHandleSchema getSchemaHandler() {

    return mDDFManager.getSchemaHandler();
  }

  public IHandleStreamingData getStreamingDataHandler() {

    return mDDFManager.getStreamingDataHandler();
  }

  public IHandleTimeSeries getTimeSeriesHandler() {

    return mDDFManager.getTimeSeriesHandler();
  }

  public IHandleViews getViewHandler() {

    return mDDFManager.getViewHandler();
  }

}
