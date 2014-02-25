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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.etl.IHandleSql;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.analytics.IAlgorithm;
import com.adatao.ddf.analytics.IAlgorithmOutputModel;
import com.adatao.ddf.analytics.IComputeBasicStatistics;
import com.adatao.ddf.analytics.IRunAlgorithms;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.content.IHandleIndexing;
import com.adatao.ddf.content.IHandleMetaData;
import com.adatao.ddf.content.IHandleMissingData;
import com.adatao.ddf.content.IHandleMutability;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.IHandleSchema;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;
import com.google.common.base.Strings;

/**
 * <p>
 * A Distributed DDF (DDF) has a number of key properties (metadata,
 * representations, etc.) and capabilities (self-compute basic statistics,
 * aggregations, etc.).
 * </p>
 * <p>
 * This class was designed using the Bridge Pattern to provide clean separation
 * between the abstract concepts and the implementation so that the API can
 * support multiple big data platforms under the same set of abstract concepts.
 * </p>
 * 
 * @author ctn
 * 
 */
public class DDF extends ALoggable implements ISupportPhantomReference {

  /**
   * 
   * @param data
   *          The DDF data
   * @param elementType
   *          The DDF data is expected to have rows (or columns) of elements with elementType
   * @param namespace
   *          The namespace to place this DDF in. If null, it will be picked up from the
   *          DDFManager's current namespace.
   * @param name
   *          The name for this DDF. If null, it will come from the given schema. If that's null, a
   *          UUID-based name will be generated.
   * @param schema
   *          The {@link Schema} of the new DDF
   * @throws DDFException
   */
  public DDF(DDFManager manager, Object data, Class<?> elementType, String namespace, String name, Schema schema)
      throws DDFException {

    this.initialize(manager, data, elementType, namespace, name, schema);
  }

  protected DDF() {
  }

  protected void initialize(DDFManager manager, Object data, Class<?> elementType, String namespace, String name,
      Schema schema) throws DDFException {

  private ADDFManager mManager;

    if (Strings.isNullOrEmpty(name) && schema != null) name = schema.getTableName();
    if (Strings.isNullOrEmpty(name)) name = String.format("DDF-%s", UUID.randomUUID());
    this.setName(name);
  }

  private static ADDFManager sDefaultManager;
  /**
   * @return the name of this DDF
   */
  public String getName() {
    return mName;
  }

  /**
   * @param name
   *          the DDF name to set
   */
  public void setName(String name) {
    this.mName = name;
  }

  private static DDFConfig.Config sConfig;

  protected static DDFConfig.Config getConfig() {
    if (sConfig == null)
      try {
        initialize();
      } catch (Exception e) {
        sLOG.error("Unable to initialize DDF", e);
      }

  public DDFManager getManager() {
    return this.mManager;
  }

  public static final String DEFAULT_CONFIG_FILE_NAME = "ddf.ini";


  // ////// MetaData that deserves to be right here at the top level ////////
  /**
   * Returns the currently set global DDF engine, e.g., "spark".
   * <p>
   * The global DDF engine is the one that will be used when static DDF methods
   * are invoked, e.g., {@link DDF#sql2ddf(String)}. This makes it convenient
   * for users who are only using one DDF engine at a time, which should be 90%
   * of all use cases.
   * <p>
   * It is still possible to use multiple DDF engines simultaneously, by
   * invoking individual instances of {@link IDDFManager}, e.g.,
   * SparkDDFManager.
   * 
   * @return
   */
  public static String getEngine() {
    return sDDFEngine;
  }

  public String getTableName() {
    return this.getSchema().getTableName();
  }
    try {
      // Also load/reload the ddf.ini file
      if (sConfig != null)
        sConfig.reset();
      sConfig = DDFConfig.loadConfig();

      // And set the global default DDF manager corresponding to the ddfEngine
      // we're given
      String className = sConfig.get(ddfEngine).get("DDFManager");
      if (className == null)
        throw new DDFException(
            "Cannot locate DDFManager class name for engine " + ddfEngine);

      Class<?> managerClass = Class.forName(className);
      if (managerClass == null)
        throw new DDFException("Cannot locate class for name " + className);

  // ////// Function-Group Handlers ////////

  private IComputeBasicStatistics mBasicStatisticsComputer;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetaData mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandleSql mSqlHandler;
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

  public DDF setBasicStatisticsComputer(IComputeBasicStatistics aBasicStatisticsComputer) {
    this.mBasicStatisticsComputer = aBasicStatisticsComputer;
    return this;
  }

  protected IComputeBasicStatistics createBasicStatisticsComputer() {
    return newHandler(IComputeBasicStatistics.class);
  }


  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) mIndexingHandler = this.createIndexingHandler();
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public DDF setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
    return this;
  }

  protected IHandleIndexing createIndexingHandler() {
    return newHandler(IHandleIndexing.class);
  }


  public IHandleJoins getJoinsHandler() {
    if (mJoinsHandler == null) mJoinsHandler = this.createJoinsHandler();
    if (mJoinsHandler == null) throw new UnsupportedOperationException();
    else return mJoinsHandler;
  }

  public DDF setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
    return this;
  }

  protected IHandleJoins createJoinsHandler() {
    return newHandler(IHandleJoins.class);
  }


  public IHandleMetaData getMetaDataHandler() {
    if (mMetaDataHandler == null) mMetaDataHandler = this.createMetaDataHandler();
    if (mMetaDataHandler == null) throw new UnsupportedOperationException();
    else return mMetaDataHandler;
  }

  public DDF setMetaDataHandler(IHandleMetaData aMetaDataHandler) {
    this.mMetaDataHandler = aMetaDataHandler;
    return this;
  }

  protected IHandleMetaData createMetaDataHandler() {
    return newHandler(IHandleMetaData.class);
  }


  public IHandleMiscellany getMiscellanyHandler() {
    if (mMiscellanyHandler == null) mMiscellanyHandler = this.createMiscellanyHandler();
    if (mMiscellanyHandler == null) throw new UnsupportedOperationException();
    else return mMiscellanyHandler;
  }

  public DDF setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
    return this;
  }

  protected IHandleMiscellany createMiscellanyHandler() {
    return newHandler(IHandleMiscellany.class);
  }


  public IHandleMissingData getMissingDataHandler() {
    if (mMissingDataHandler == null) mMissingDataHandler = this.createMissingDataHandler();
    if (mMissingDataHandler == null) throw new UnsupportedOperationException();
    else return mMissingDataHandler;
  }

  public DDF setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
    return this;
  }

  protected IHandleMissingData createMissingDataHandler() {
    return newHandler(IHandleMissingData.class);
  }


  public IHandleMutability getMutabilityHandler() {
    if (mMutabilityHandler == null) mMutabilityHandler = this.createMutabilityHandler();
    if (mMutabilityHandler == null) throw new UnsupportedOperationException();
    else return mMutabilityHandler;
  }

  public DDF setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
    return this;
  }

  protected IHandleMutability createMutabilityHandler() {
    return newHandler(IHandleMutability.class);
  }


  public IHandleSql getSqlHandler() {
    if (mSqlHandler == null) mSqlHandler = this.createSqlHandler();
    if (mSqlHandler == null) throw new UnsupportedOperationException();
    else return mSqlHandler;
  }

  public DDF setSqlHandler(IHandleSql ASqlHandler) {
    this.mSqlHandler = ASqlHandler;
    return this;
  }

  protected IHandleSql createSqlHandler() {
    return newHandler(IHandleSql.class);
  }

  public IHandleRepresentations getRepresentationHandler() {
    if (mRepresentationHandler == null) mRepresentationHandler = this.createRepresentationHandler();
    if (mRepresentationHandler == null) throw new UnsupportedOperationException();
    else return mRepresentationHandler;
  }

  public DDF setRepresentationHandler(IHandleRepresentations aRepresentationHandler) {
    this.mRepresentationHandler = aRepresentationHandler;
    return this;
  }

  protected IHandleRepresentations createRepresentationHandler() {
    return newHandler(IHandleRepresentations.class);
  }


  public IHandleReshaping getReshapingHandler() {
    if (mReshapingHandler == null) mReshapingHandler = this.createReshapingHandler();
    if (mReshapingHandler == null) throw new UnsupportedOperationException();
    else return mReshapingHandler;
  }

  public DDF setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
    return this;
  }

  // ////// MetaData that deserves to be right here at the top level ////////

  public DDF setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
    return this;
  }

  protected IHandleSchema createSchemaHandler() {
    return newHandler(IHandleSchema.class);
  }


  public IHandleStreamingData getStreamingDataHandler() {
    if (mStreamingDataHandler == null) mStreamingDataHandler = this.createStreamingDataHandler();
    if (mStreamingDataHandler == null) throw new UnsupportedOperationException();
    else return mStreamingDataHandler;
  }

  public DDF setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
    return this;
  }

  protected IHandleStreamingData createStreamingDataHandler() {
    return newHandler(IHandleStreamingData.class);
  }


  // Calculate summary statistics of the DDF
  public Summary[] getSummary() {
    return this.getBasicStatisticsComputer().getSummary();
  }

  // Get a specific representation
  public Object getRepresentation(Class<?> elementType) {
    return this.getRepresentationHandler().get(elementType);
  }
  
  public Object getDefaultRepresentation() {
    return this.getRepresentationHandler().getDefault();
  }

  // Run Algorithms
  public IAlgorithmOutputModel train(IAlgorithm algorithm) {
    return this.getAlgorithmRunner().run(algorithm, this);
  }

  // ////// Static convenient methods for IHandleSql ////////

  public IHandleViews getViewHandler() {
    if (mViewHandler == null) mViewHandler = this.createViewHandler();
    if (mViewHandler == null) throw new UnsupportedOperationException();
    else return mViewHandler;
  }

  public DDF setViewHandler(IHandleViews aViewHandler) {
    this.mViewHandler = aViewHandler;
    return this;
  }


  public static DDF sql2ddf(String command, DataFormat dataFormat)
      throws DDFException {
    return getDefaultManager().sql2ddf(command, dataFormat);
  }

  public static DDF sql2ddf(String command, Schema schema, String dataSource)
      throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataSource);
  }

  public static DDF sql2ddf(String command, Schema schema, DataFormat dataFormat)
      throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataFormat);
  }

  public static DDF sql2ddf(String command, Schema schema, String dataSource,
      DataFormat dataFormat) throws DDFException {
    return getDefaultManager().sql2ddf(command, schema, dataSource, dataFormat);
  }

  public static List<String> sql2txt(String command) throws DDFException {
    return getDefaultManager().sql2txt(command);
  }

  public static List<String> sql2txt(String command, String dataSource)
      throws DDFException {
    return getDefaultManager().sql2txt(command, dataSource);
  }

  // /**
  // * Get factor for a column
  // *
  // * @param columnIndex
  // * @return list of levels for specified column
  // */
  // public List<String> factorize(int columnIndex) {
  // AFactorHandler.FactorColumnInfo[] factors =
  // this.getFactorHandler().factorize(new int[] {
  // columnIndex });
  //
  // // IMPLEMENTATION HERE
  // return null;
  // }
  //
  // /**
  // * Get factor for list of columns
  // *
  // * @param columnIDs
  // * @return a hashmap contain mapping from columnID -> list of levels
  // */
  // public HashMap<Integer, List<String>> factorize(int[] columnIndices) {
  // AFactorHandler.FactorColumnInfo[] factors =
  // this.getFactorHandler().factorize(columnIndices);
  //
  // // IMPLEMENTATION HERE
  // return null;
  // }
  //
  // /**
  // * apply factor coding for a specified column
  // *
  // * @param columnID
  // * @return new DDF with factor coding applied
  // */
  // public DDF applyFactorCoding(int columnID) {
  // return getManager().getFactorSupporter().applyFactorCoding(columnID, this);
  // }
  //
  // /**
  // * apply factor coding for list of columns
  // */
  // public DDF applyFactorCoding(int[] columnIDs) {
  // return getManager().getFactorSupporter().applyFactorCoding(columnIDs,
  // this);
  // }
  /**
   * Instantiate a new {@link ADDFFunctionalGroupHandler} given its class name
   * 
   * @param className
   * @return
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   * @throws SecurityException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */

  public static DDF newDDF(String name, ColumnWithData columnWithData)
      throws DDFException {
    return newDDF(name, new ColumnWithData[] { columnWithData });
  }

  public static DDF newDDF(String name, ColumnWithData[] columnsWithData)
      throws DDFException {
    return getDefaultManager().newDDF(name, columnsWithData);
  }
}
