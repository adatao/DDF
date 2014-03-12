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
import java.util.List;
import com.adatao.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import com.adatao.ddf.analytics.AggregationHandler.AggregateField;
import com.adatao.ddf.analytics.AggregationHandler.AggregationResult;
import com.adatao.ddf.analytics.ISupportStatistics;
import com.adatao.ddf.analytics.IHandleAggregation;
import com.adatao.ddf.analytics.ISupportML;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri;
import com.adatao.ddf.content.ISerializable;
import com.adatao.ddf.content.IHandleIndexing;
import com.adatao.ddf.content.IHandleMetaData;
import com.adatao.ddf.content.IHandleMissingData;
import com.adatao.ddf.content.IHandleMutability;
import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.content.IHandlePersistence.IPersistible;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.IHandleSchema;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.etl.IHandleSql;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.facades.MLFacade;
import com.adatao.ddf.facades.ViewsFacade;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.misc.ALoggable;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.misc.IHandleMiscellany;
import com.adatao.ddf.misc.IHandleStreamingData;
import com.adatao.ddf.misc.IHandleTimeSeries;
import com.adatao.ddf.types.IGloballyAddressable;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;
import com.adatao.local.ddf.LocalDDFManager;
import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;


/**
 * <p>
 * A Distributed DDF (DDF) has a number of key properties (metadata, representations, etc.) and capabilities
 * (self-compute basic statistics, aggregations, etc.).
 * </p>
 * <p>
 * This class was designed using the Bridge Pattern to provide clean separation between the abstract concepts and the
 * implementation so that the API can support multiple big data platforms under the same set of abstract concepts.
 * </p>
 * 
 * @author ctn
 * 
 */
public abstract class DDF extends ALoggable //
    implements IGloballyAddressable, IPersistible, ISupportPhantomReference, ISerializable {

  private static final long serialVersionUID = -2198317495102277825L;


  /**
   * 
   * @param data
   *          The DDF data
   * @param namespace
   *          The namespace to place this DDF in. If null, it will be picked up from the DDFManager's current namespace.
   * @param name
   *          The name for this DDF. If null, it will come from the given schema. If that's null, a UUID-based name will
   *          be generated.
   * @param schema
   *          The {@link Schema} of the new DDF
   * @throws DDFException
   */
  public DDF(DDFManager manager, Object data, Class<?> containerType, Class<?> unitType, String namespace, String name,
      Schema schema) throws DDFException {

    this.initialize(manager, data, containerType, unitType, namespace, name, schema);
  }

  protected DDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
    this(manager != null ? manager : defaultManagerIfNull, null, null, null, null, null, null);
  }

  /**
   * This is intended primarily to provide a dummy DDF only. This signature must be provided by each implementor.
   * 
   * @param manager
   * @throws DDFException
   */
  protected DDF(DDFManager manager) throws DDFException {
    this(manager, sDummyManager);
  }

  /**
   * Available for run-time instantiation only.
   * 
   * @throws DDFException
   */
  protected DDF() throws DDFException {
    this(sDummyManager);
  }

  /**
   * Initialization to be done after constructor assignments, such as setting of the all-important DDFManager.
   */
  protected void initialize(DDFManager manager, Object data, Class<?> containerType, Class<?> unitType,
      String namespace, String name, Schema schema) throws DDFException {

    this.setManager(manager); // this must be done first in case later stuff needs a manager

    this.getRepresentationHandler().set(data, containerType, unitType);

    this.getSchemaHandler().setSchema(schema);
    if (Strings.isNullOrEmpty(name) && schema != null) name = schema.getTableName();

    if (Strings.isNullOrEmpty(namespace)) namespace = this.getManager().getNamespace();
    this.setNamespace(namespace);

    this.setName(name);

    // Facades
    this.ML = new MLFacade(this, this.getMLSupporter());
    this.Views = new ViewsFacade(this, this.getViewHandler());
  }


  // ////// Instance Fields & Methods ////////


  @Expose private String mNamespace;

  @Expose private String mName;


  /**
   * @return the namespace this DDF belongs in
   * @throws DDFException
   */
  @Override
  public String getNamespace() {
    if (mNamespace == null) {
      try {
        mNamespace = this.getManager().getNamespace();
      } catch (Exception e) {
        mLog.warn("Cannot retrieve namespace for DDF " + this.getName(), e);
      }
    }
    return mNamespace;
  }

  /**
   * @param namespace
   *          the namespace to place this DDF in
   */
  @Override
  public void setNamespace(String namespace) {
    this.mNamespace = namespace;
  }

  /**
   * Also synchronizes the Schema's table name with that of the DDF name
   * 
   * @return the name of this DDF
   */
  @Override
  public String getName() {
    if (Strings.isNullOrEmpty(mName)) {
      if (!Strings.isNullOrEmpty(this.getSchemaHandler().getTableName())) {
        mName = this.getSchemaHandler().getTableName();

      } else {
        mName = this.getSchemaHandler().newTableName();
        if (this.getSchemaHandler().getSchema() != null) {
          this.getSchemaHandler().getSchema().setTableName(mName);
        }
      }
    }

    return mName;
  }

  /**
   * @param name
   *          the DDF name to set
   */
  @Override
  public void setName(String name) {
    this.mName = name;
  }



  /**
   * We provide a "dummy" DDF Manager in case our manager is not set for some reason. (This may lead to nothing good).
   */
  private static final DDFManager sDummyManager = new LocalDDFManager();

  private DDFManager mManager;


  /**
   * Returns the previously set manager, or sets it to a dummy manager if null. We provide a "dummy" DDF Manager in case
   * our manager is not set for some reason. (This may lead to nothing good).
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

  public Column getColumn(String column) {
    return this.getSchema().getColumn(column);
  }

  public String getTableName() {
    return this.getSchema().getTableName();
  }

  public List<String> getColumnNames() {
    return this.getSchema().getColumnNames();
  }


  public long getNumRows() {
    return this.getMetaDataHandler().getNumRows();
  }

  public int getNumColumns() {
    return this.getSchemaHandler().getNumColumns();
  }



  // ///// Generate DDF views

  public ViewsFacade Views;



  // ///// Aggregate operations

  /**
   * 
   * @param columnA
   * @param columnB
   * @return correlation value of columnA and columnB
   * @throws DDFException
   */
  public double correlation(String columnA, String columnB) throws DDFException {
    return this.getAggregationHandler().computeCorrelation(columnA, columnB);
  }

  /**
   * Compute aggregation which is equivalent to SQL aggregation statement like
   * "SELECT a, b, sum(c), max(d) FROM e GROUP BY a, b"
   * 
   * @param fields
   *          a string includes aggregated fields and functions, e.g "a, b, sum(c), max(d)"
   * @return
   * @throws DDFException
   */
  public AggregationResult aggregate(String fields) throws DDFException {
    return this.getAggregationHandler().aggregate(AggregateField.fromSqlFieldSpecs(fields));
  }


  // ////// Function-Group Handlers ////////

  private ISupportStatistics mStatisticsSupporter;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetaData mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandleSql mSqlHandler;
  private IHandlePersistence mPersistenceHandler;
  private IHandleRepresentations mRepresentationHandler;
  private IHandleReshaping mReshapingHandler;
  private IHandleSchema mSchemaHandler;
  private IHandleStreamingData mStreamingDataHandler;
  private IHandleTimeSeries mTimeSeriesHandler;
  private IHandleViews mViewHandler;
  private ISupportML mMLSupporter;
  private IHandleAggregation mAggregationHandler;



  public ISupportStatistics getStatisticsSupporter() {
    if (mStatisticsSupporter == null) mStatisticsSupporter = this.createStatisticsSupporter();
    if (mStatisticsSupporter == null) throw new UnsupportedOperationException();
    else return mStatisticsSupporter;
  }

  public DDF setStatisticsSupporter(ISupportStatistics aStatisticsSupporter) {
    this.mStatisticsSupporter = aStatisticsSupporter;
    return this;
  }

  protected ISupportStatistics createStatisticsSupporter() {
    return newHandler(ISupportStatistics.class);
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

  public IHandleAggregation getAggregationHandler() {
    if (mAggregationHandler == null) mAggregationHandler = this.createAggregationHandler();
    if (mAggregationHandler == null) throw new UnsupportedOperationException();
    else return mAggregationHandler;
  }

  public DDF setAggregationHandler(IHandleAggregation aAggregationHandler) {
    this.mAggregationHandler = aAggregationHandler;
    return this;
  }

  protected IHandleAggregation createAggregationHandler() {
    return newHandler(IHandleAggregation.class);
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

  public DDF setSqlHandler(IHandleSql aSqlHandler) {
    this.mSqlHandler = aSqlHandler;
    return this;
  }

  protected IHandleSql createSqlHandler() {
    return newHandler(IHandleSql.class);
  }


  public IHandlePersistence getPersistenceHandler() {
    if (mPersistenceHandler == null) mPersistenceHandler = this.createPersistenceHandler();
    if (mPersistenceHandler == null) throw new UnsupportedOperationException();
    else return mPersistenceHandler;
  }

  public DDF setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
    return this;
  }

  protected IHandlePersistence createPersistenceHandler() {
    return newHandler(IHandlePersistence.class);
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

  public ISupportML getMLSupporter() {
    if (mMLSupporter == null) mMLSupporter = this.createMLSupporter();
    if (mMLSupporter == null) throw new UnsupportedOperationException();
    else return mMLSupporter;
  }

  public DDF setMLSupporter(ISupportML aMLSupporter) {
    this.mMLSupporter = aMLSupporter;
    return this;
  }

  protected ISupportML createMLSupporter() {
    return newHandler(ISupportML.class);
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
      className = Config.getValueWithGlobalDefault(this.getEngine(), theInterface.getSimpleName());

      if (Strings.isNullOrEmpty(className)) {
        mLog.error(String.format("Cannot determine classname for %s from configuration source [%s] %s",
            theInterface.getSimpleName(), Config.getConfigHandler().getSource(), this.getEngine()));
        return null;
      }

      Class<?> clazz = Class.forName(className);
      Constructor<ADDFFunctionalGroupHandler> cons = (Constructor<ADDFFunctionalGroupHandler>) clazz
          .getDeclaredConstructor(new Class<?>[] { DDF.class });

      if (cons != null) cons.setAccessible(true);

      return cons != null ? (I) cons.newInstance(this) : null;

    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException
        | InvocationTargetException e) {

      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), e);
      return null;
    }


  }


  public String getUri() {
    return String.format("%s://%s/%s", this.getEngine(), this.getNamespace(), this.getName());
  }

  @Override
  public String toString() {
    return this.getUri();
  }



  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was registered under
   * {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    // @formatter:off
    this
      .setMLSupporter(null)
      .setStatisticsSupporter(null)
      .setIndexingHandler(null)
      .setJoinsHandler(null)
      .setMetaDataHandler(null)
      .setMiscellanyHandler(null)
      .setMissingDataHandler(null)
      .setMutabilityHandler(null)
      .setSqlHandler(null)
      .setPersistenceHandler(null)
      .setRepresentationHandler(null)
      .setReshapingHandler(null)
      .setSchemaHandler(null)
      .setStreamingDataHandler(null)
      .setTimeSeriesHandler(null)
      ;
    // @formatter:on
  }



  // ////// Facade methods ////////


  // //// IHandleViews //////

  /**
   * 
   * @param columnName
   * @return
   */
  public int getColumnIndex(String columnName) {
    return this.getSchema().getColumnIndex(columnName);
  }

  // public <T> Iterator<T> getRowIterator(Class<T> dataType) {
  // return this.getViewHandler().getRowIterator(dataType);
  // }
  //
  // public Iterator<?> getRowIterator() {
  // return this.getViewHandler().getRowIterator();
  // }
  //
  // public <D, C> Iterator<C> getElementIterator(Class<D> dataType, Class<C> columnType, String columnName) {
  // return this.getViewHandler().getElementIterator(dataType, columnType, columnName);
  // }
  //
  // public Iterator<?> getElementIterator(String columnName) {
  // return this.getViewHandler().getElementIterator(columnName);
  // }



  // //// ISupportStatistics //////

  // Calculate summary statistics of the DDF
  public Summary[] getSummary() throws DDFException {
    return this.getStatisticsSupporter().getSummary();
  }

  public FiveNumSummary[] getFiveNumSummary() throws DDFException {
    return this.getStatisticsSupporter().getFiveNumSummary(this.getColumnNames());
  }



  // //// ISupportML //////

  public MLFacade ML;



  // //// IHandlePersistence //////

  public PersistenceUri persist() throws DDFException {
    return this.persist(true);
  }

  @Override
  public PersistenceUri persist(boolean doOverwrite) throws DDFException {
    return this.getPersistenceHandler().persist(doOverwrite);
  }

  @Override
  public void unpersist() throws DDFException {
    this.getManager().unpersist(this.getNamespace(), this.getName());
  }


  /**
   * The base implementation checks if the schema is null, and if so, generate a generic one. This is useful/necessary
   * before persistence, to avoid the situtation of null schemas being persisted.
   */
  @Override
  public void beforePersisting() {
    if (this.getSchema() == null) this.getSchemaHandler().setSchema(this.getSchemaHandler().generateSchema());
  }

  @Override
  public void afterPersisting() {}

  @Override
  public void beforeUnpersisting() {}

  @Override
  public void afterUnpersisting() {}



  // //// ISerializable //////

  @Override
  public void beforeSerialization() throws DDFException {}

  @Override
  public void afterSerialization() throws DDFException {}

  @Override
  public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData)
      throws DDFException {
    return deserializedObject;
  }
}
