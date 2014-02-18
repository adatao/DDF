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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.etl.IHandleSql;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.analytics.IAlgorithmOutputModel;
import com.adatao.ddf.analytics.IComputeBasicStatistics;
import com.adatao.ddf.analytics.IAlgorithm;
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

  private static final Logger sLOG = LoggerFactory.getLogger(DDF.class);

  static {
    try {
      initialize();
    } catch (Exception e) {
      sLOG.error("Error during DDF initialization", e);
    }
  }

  private static void initialize() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    loadConfig();
  }

  public static void shutdown() {
    getDefaultManager().shutdown();
  }



  private ADDFManager mManager;

  public ADDFManager getManager() {
    return this.mManager;
  }

  protected void setManager(ADDFManager aDDFManager) {
    this.mManager = aDDFManager;
  }


  private static ADDFManager sDefaultManager;

  public static ADDFManager getDefaultManager() {
    return sDefaultManager;
  }

  protected static void setDefaultManager(ADDFManager aDDFManager) {
    sDefaultManager = aDDFManager;
  }



  /**
   * Stores DDF configuration information from ddf.cfg
   */
  protected static class Config {

    private Map<String, Map<String, String>> sSections;

    public Config() {
      this.reset();
    }

    private String safeToLower(String s) {
      return s == null ? null : s.toLowerCase();
    }

    public String get(String sectionName, String key) {
      Map<String, String> section = this.getSection(safeToLower(sectionName));
      return section == null ? null : section.get(safeToLower(sectionName));
    }

    public void set(String sectionName, String key, String value) {
      Map<String, String> section = this.getOrCreateSection(safeToLower(sectionName));
      section.put(safeToLower(sectionName), value);
    }

    public void remove(String sectionName, String key) {
      Map<String, String> section = this.getSection(safeToLower(sectionName));
      if (section != null) section.remove(safeToLower(sectionName));
    }

    public Map<String, String> getSection(String sectionName) {
      return sSections.get(safeToLower(sectionName));
    }

    public Map<String, String> getOrCreateSection(String sectionName) {
      Map<String, String> section = this.getSection(safeToLower(sectionName));

      if (section == null) {
        section = new HashMap<String, String>();
        sSections.put(safeToLower(sectionName), section);
      }

      return section;
    }

    public void removeSection(String sectionName) {
      sSections.remove(safeToLower(sectionName));
    }

    public void reset() {
      sSections = new HashMap<String, Map<String, String>>();
    }
  }

  private static final Config sConfig = new Config();

  protected static Config getConfig() {
    return sConfig;
  }

  public static final String DEFAULT_DDF_ENGINE = "spark";

  private static String sDDFEngine = DEFAULT_DDF_ENGINE;

  public static String getDDFEngine() {
    return sDDFEngine;
  }

  public static void setDDFEngine(String ddfEngine) {
    sDDFEngine = ddfEngine;
  }

  /**
   * Load configuration from ddf.conf
   * 
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public static void loadConfig() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    // Temporary code for now; this should really be in ddf.cfg
    String ddfEngine = getDDFEngine();
    sConfig.set(ddfEngine, "DDF", "com.adatao.spark.ddf.SparkDDF");
    sConfig.set(ddfEngine, "DDFManager", "com.adatao.spark.ddf.SparkDDFManager");
    sConfig.set(ddfEngine, "IHandleRepresentations", "com.adatao.spark.ddf.RepresentationHandler");
    sConfig.set(ddfEngine, "IHandleViews", "com.adatao.spark.ddf.ViewHandler");
    sConfig.set(ddfEngine, "IHandleMetaData", "com.adatao.spark.ddf.MetaDataHandler");
    sConfig.set(ddfEngine, "IHandleSchema", "com.adatao.spark.ddf.SchemaHandler");
    sConfig.set(ddfEngine, "IHandleSql", "com.adatao.spark.ddf.SqlHandler");
    sConfig.set(ddfEngine, "IRunAlgorithms", "com.adatao.spark.ddf.AlgorithmRunner");


    ADDFManager defaultManager = (ADDFManager) Class.forName(sConfig.get(ddfEngine, "DDFManager")).newInstance();
    setDefaultManager(defaultManager);
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

  public IHandleSql getSqlHandler() {
    return this.getManager().getSqlHandler();
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

  // Run Algorithms
  public IAlgorithmOutputModel train(IAlgorithm algorithm) {
    return this.getAlgorithmRunner().run(algorithm, this);
  }



  // ////// Static convenient methods for IHandleSql ////////

  public static DDF sql2ddf(String command) throws DDFException {
    return getDefaultManager().sql2ddf(command);
  }

  public static DDF sql2ddf(String command, Schema schema) throws DDFException {
    return getDefaultManager().sql2ddf(command, schema);
  }

  public static DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return getDefaultManager().sql2ddf(command, dataFormat);
  }

  public static DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataSource);
  }

  public static DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataFormat);
  }

  public static DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat)
      throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataSource, dataFormat);
  }

  public static List<String> sql2txt(String command) throws DDFException {
    return getDefaultManager().sql2txt(command);
  }

  public static List<String> sql2txt(String command, String dataSource) throws DDFException {
    return getDefaultManager().sql2txt(command, dataSource);
  }
}
