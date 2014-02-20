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

import com.adatao.ddf.content.ColumnInfo;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

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

  // static {
  // try {
  // initialize();
  // } catch (Exception e) {
  // sLOG.error("Error during DDF initialization", e);
  // }
  // }

  public static void initialize() throws DDFException {
    initialize(DEFAULT_DDF_ENGINE);
  }

  public static void initialize(String ddfEngine) throws DDFException {
    setDDFEngine(ddfEngine);
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


  private static DDFConfig.Config sConfig;

  protected static DDFConfig.Config getConfig() {
    if (sConfig == null) try {
      initialize();
    } catch (Exception e) {
      sLOG.error("Unable to initialize DDF", e);
    }

    return sConfig;
  }


  public static final String DEFAULT_CONFIG_FILE_NAME = "ddf.ini";

  public static final String DEFAULT_DDF_ENGINE = "spark";

  private static String sDDFEngine = DEFAULT_DDF_ENGINE;

  /**
   * Returns the currently set global DDF engine, e.g., "spark".
   * <p>
   * The global DDF engine is the one that will be used when static DDF methods are invoked, e.g.,
   * {@link DDF#sql2ddf(String)}. This makes it convenient for users who are only using one DDF
   * engine at a time, which should be 90% of all use cases.
   * <p>
   * It is still possible to use multiple DDF engines simultaneously, by invoking individual
   * instances of {@link IDDFManager}, e.g., SparkDDFManager.
   * 
   * @return
   */
  public static String getDDFEngine() {
    return sDDFEngine;
  }

  /**
   * Sets the desired global DDF engine, e.g., "spark"
   * 
   * @param ddfEngine
   * @throws DDFException
   */
  public static void setDDFEngine(String ddfEngine) throws DDFException {
    sDDFEngine = ddfEngine;

    try {
      // Also load/reload the ddf.ini file
      if (sConfig != null) sConfig.reset();
      sConfig = DDFConfig.loadConfig();

      // And set the global default DDF manager corresponding to the ddfEngine we're given
      String className = sConfig.get(ddfEngine).get("DDFManager");
      if (className == null) throw new DDFException("Cannot locate DDFManager class name for engine " + ddfEngine);

      Class<?> managerClass = Class.forName(className);
      if (managerClass == null) throw new DDFException("Cannot locate class for name " + className);

      ADDFManager defaultManager = (ADDFManager) managerClass.newInstance();
      setDefaultManager(defaultManager);

    } catch (Exception e) {
      throw new DDFException("Error in setting DDF Engine", e);
    }
  }

  // ////// ADDFManager delegates ////////


  public long nrows(){
    return this.getHelper().getMetaDataHandler().getNumRows();
  }
  
  public long ncols(){
    return this.getHelper().getMetaDataHandler().getNumColumns();
  }
    
  public ColumnInfo[] getColumnMetadata(){
    return this.getHelper().getSchemaHandler().getColumnMetadata();
  }
  
  public DDF getRandomSample(int numSamples) {
    return this.getHelper().getMiscellanyHandler().getRandomSample(this, numSamples);

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
