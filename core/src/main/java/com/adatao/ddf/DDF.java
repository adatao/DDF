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
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandleDataCommands;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;


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
public class DDF implements IHandleDataCommands {

  private ADDFManager mManager;

  public ADDFManager getDDFManager() {
    return this.mManager;
  }

  protected void setManager(ADDFManager aDDFManager) {
    this.mManager = aDDFManager;
  }


  public IComputeBasicStatistics getBasicStatisticsComputer() {
    return this.getDDFManager().getBasicStatisticsComputer();
  }

  public IHandleIndexing getIndexingHandler() {
    return this.getDDFManager().getIndexingHandler();
  }

  public IHandleJoins getJoinsHandler() {
    return this.getDDFManager().getJoinsHandler();
  }

  public IHandleMetaData getMetaDataHandler() {
    return this.getDDFManager().getMetaDataHandler();
  }

  public IHandleMiscellany getMiscellanyHandler() {
    return this.getDDFManager().getMiscellanyHandler();
  }

  public IHandleMissingData getMissingDataHandler() {
    return this.getDDFManager().getMissingDataHandler();
  }

  public IHandleMutability getMutabilityHandler() {
    return this.getDDFManager().getMutabilityHandler();
  }

  public IHandleDataCommands getDataCommandHandler() {
    return this.getDDFManager().getDataCommandHandler();
  }

  public IHandleRepresentations getRepresentationHandler() {
    return this.getDDFManager().getRepresentationHandler();
  }

  public IHandleReshaping getReshapingHandler() {
    return this.getDDFManager().getReshapingHandler();
  }

  public IHandleSchema getSchemaHandler() {
    return this.getDDFManager().getSchemaHandler();
  }

  public IHandleStreamingData getStreamingDataHandler() {
    return this.getDDFManager().getStreamingDataHandler();
  }

  public IHandleTimeSeries getTimeSeriesHandler() {
    return this.getDDFManager().getTimeSeriesHandler();
  }

  public IHandleViews getViewHandler() {
    return this.getDDFManager().getViewHandler();
  }

  public IRunAlgorithms getAlgorithmRunner() {
    return this.getDDFManager().getAlgorithmRunner();
  }



  // ////// IHandleDataCommands ////////

  protected final String TABLE_NAME_PATTERN = "(?i)<table>";

  protected String regexTableName(String command) {
    if (command != null && command.length() > 0) {
      command = command.replaceAll(TABLE_NAME_PATTERN, this.getTableName());
    }

    return command;
  }

  @Override
  public DDF cmd2ddf(String command) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command));
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command), schema);
  }

  @Override
  public DDF cmd2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command), dataFormat);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command), schema, dataSource);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command), schema, dataFormat);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(this.regexTableName(command), schema, dataSource, dataFormat);
  }


  @Override
  public List<String> cmd2txt(String command) throws DDFException {
    return this.getDataCommandHandler().cmd2txt(this.regexTableName(command));
  }

  @Override
  public List<String> cmd2txt(String command, String dataSource) throws DDFException {
    return this.getDataCommandHandler().cmd2txt(this.regexTableName(command), dataSource);
  }



  // ////// MetaData that deserves to be right here at the top level ////////

  public Schema getSchema() {
    return this.getSchemaHandler().getSchema();
  }

  public String getTableName() {
    return this.getSchema().getTableName();
  }

  public long getNumRows() {
    return this.getMetaDataHandler().getNumRows();
  }

  public long getNumColumns() {
    return this.getSchemaHandler().getNumColumns();
  }
}
