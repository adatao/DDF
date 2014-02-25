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
import com.adatao.ddf.util.ConfigHandler;
import com.adatao.ddf.util.IHandleConfig;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;
import com.adatao.jcoll.ddf.JCollDDFManager;
import com.google.common.base.Strings;


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
public class DDF extends ALoggable implements ISupportPhantomReference {

  /**
   * 
   * @param data
   *          The DDF data
   * @param rowType
   *          The DDF data is expected to have rows (or columns) of elements with rowType
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
  public DDF(DDFManager manager, Object data, Class<?> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    this.initialize(manager, data, rowType, namespace, name, schema);
  }

  protected DDF() {
  }

  protected void initialize(DDFManager manager, Object data, Class<?> rowType, String namespace, String name,
      Schema schema) throws DDFException {

    this.setManager(manager); // this must be done first in case later stuff needs a manager

    this.getRepresentationHandler().set(data, rowType);

    this.getSchemaHandler().setSchema(schema);
    if (Strings.isNullOrEmpty(name) && schema != null) name = schema.getTableName();

    if (Strings.isNullOrEmpty(namespace)) namespace = this.getManager().getNamespace();
    this.setNamespace(namespace);
    if (Strings.isNullOrEmpty(name)) name = String.format("DDF-%s", UUID.randomUUID());
    this.setName(name);
  }



  // ////// Global/Static Fields & Methods ////////

  private static final IHandleConfig sConfigHandler = new ConfigHandler();

  public static IHandleConfig getConfigHandler() {
    return sConfigHandler;
  }

  public static String getConfigValue(String section, String key) {
    return getConfigHandler().getValue(section, key);
  }

  // ////// Instance Fields & Methods ////////


  private String mNamespace;

  private String mName;

  /**
   * @return the namespace this DDF belongs in
   * @throws DDFException
   */
  public String getNamespace() throws DDFException {
    if (mNamespace == null) mNamespace = this.getManager().getNamespace();
    return mNamespace;
  }

  /**
   * @param namespace
   *          the namespace to place this DDF in
   */
  public void setNamespace(String namespace) {
    this.mNamespace = namespace;
  }

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



  /**
   * We provide a "dummy" DDF Manager in case our manager is not set for some reason. (This may lead
   * to nothing good).
   */
  private DDFManager sDummyManager = new JCollDDFManager();

  private DDFManager mManager;

  /**
   * Returns the previously set manager, or sets it to a dummy manager if null. We provide a "dummy"
   * DDF Manager in case our manager is not set for some reason. (This may lead to nothing good).
   * 
   * @return
   */
  public DDFManager getManager() {
    if (mManager == null) mManager = sDummyManager;
    return mManager;
  }

  protected void setManager(DDFManager DDFManager) {
    this.mManager = DDFManager;
  }


  /**
   * 
   * @return The engine name we are built on, e.g., "spark" or "java_collections"
   */
  public String getEngine() {
    return this.getManager().getEngine();
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

  protected IHandleReshaping createReshapingHandler() {
    return newHandler(IHandleReshaping.class);
  }


  public IHandleSchema getSchemaHandler() {
    if (mSchemaHandler == null) mSchemaHandler = this.createSchemaHandler();
    if (mSchemaHandler == null) throw new UnsupportedOperationException();
    else return mSchemaHandler;
  }

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

  // Run Algorithms
  public IAlgorithmOutputModel train(IAlgorithm algorithm) {
    return this.getAlgorithmRunner().run(algorithm);
  }


  public IHandleTimeSeries getTimeSeriesHandler() {
    if (mTimeSeriesHandler == null) mTimeSeriesHandler = this.createTimeSeriesHandler();
    if (mTimeSeriesHandler == null) throw new UnsupportedOperationException();
    else return mTimeSeriesHandler;
  }

  public DDF setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
    return this;
  }

  protected IHandleTimeSeries createTimeSeriesHandler() {
    return newHandler(IHandleTimeSeries.class);
  }


  public IHandleViews getViewHandler() {
    if (mViewHandler == null) mViewHandler = this.createViewHandler();
    if (mViewHandler == null) throw new UnsupportedOperationException();
    else return mViewHandler;
  }

  public DDF setViewHandler(IHandleViews aViewHandler) {
    this.mViewHandler = aViewHandler;
    return this;
  }

  protected IHandleViews createViewHandler() {
    return newHandler(IHandleViews.class);
  }

  public IRunAlgorithms getAlgorithmRunner() {
    if (mAlgorithmRunner == null) mAlgorithmRunner = this.createAlgorithmRunner();
    if (mAlgorithmRunner == null) throw new UnsupportedOperationException();
    else return mAlgorithmRunner;
  }

  public DDF setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
    return this;
  }

  protected IRunAlgorithms createAlgorithmRunner() {
    return newHandler(IRunAlgorithms.class);
  }

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
  @SuppressWarnings("unchecked")
  protected <I> I newHandler(Class<I> theInterface) {
    if (theInterface == null) return null;

    String className = null;

    try {
      className = getConfigValue(this.getEngine(), theInterface.getSimpleName());
      if (Strings.isNullOrEmpty(className)) {
        mLog.error(String.format("Cannot determine classname for %s from configuration source [%]:%s",
            theInterface.getSimpleName(), getConfigHandler().getSource(), this.getEngine()));
        return null;
      }

      Class<?> clazz = Class.forName(className);
      Constructor<ADDFFunctionalGroupHandler> cons = (Constructor<ADDFFunctionalGroupHandler>) clazz
          .getConstructor(new Class<?>[] { DDF.class });

      return cons != null ? (I) cons.newInstance(this) : null;

    } catch (Exception e) {
      mLog.error(String.format("Cannot instantiate handler for [%s]:%s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), e);
      return null;
    }
  }



  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was
   * registered under {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    // @formatter:off
    this
      .setAlgorithmRunner(null)
      .setBasicStatisticsComputer(null)
      .setIndexingHandler(null)
      .setJoinsHandler(null)
      .setMetaDataHandler(null)
      .setMiscellanyHandler(null)
      .setMissingDataHandler(null)
      .setMutabilityHandler(null)
      .setSqlHandler(null)
      .setRepresentationHandler(null)
      .setReshapingHandler(null)
      .setSchemaHandler(null)
      .setStreamingDataHandler(null)
      .setTimeSeriesHandler(null)
      ;
    // @formatter:on
  }
}
