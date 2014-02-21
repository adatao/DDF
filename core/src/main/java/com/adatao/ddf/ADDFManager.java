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

import com.adatao.ddf.content.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.DDFConfig.Config;
import com.adatao.ddf.analytics.AFactorHandler;
import com.adatao.ddf.analytics.IComputeBasicStatistics;
import com.adatao.ddf.analytics.IHandleFactors;
import com.adatao.ddf.analytics.IRunAlgorithms;
import com.adatao.ddf.content.Schema.ColumnWithData;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandleSql;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * <p>
 * Abstract base class for a {@link DDF} implementor, which provides the support methods necessary
 * to implement various DDF interfaces, such as {@link IHandleRepresentations} and
 * {@link IRunAlgorithms}.
 * </p>
 * <p>
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to
 * provide alternative (even snap-in replacements), support/implementation for DDF. The class
 * diagram is as follows:
 * </p>
 * 
 * <pre>
 * -------------    -------------------------
 * |    DDF    |<-->|       ADDFManager      |
 * -------------    -------------------------
 *                         ^          ^
 *                         |   ...    |        -------------------
 *                         |          |------->| IHandleMetadata |
 *                         |                   -------------------
 *                         |
 *                         |        ----------------------------------
 *                         |------->| IHandleRepresentations |
 *                                  ----------------------------------
 * </pre>
 * <p>
 * An implementor need not provide all or even most of these interfaces. Each interface handler can
 * be get/set separately, as long as they cooperate properly on things like the underlying
 * representation. This makes it easy to roll out additional interfaces and their implementations
 * over time.
 * </p>
 * 
 * @author ctn
 * 
 */
public abstract class ADDFManager implements IDDFManager, ISupportPhantomReference {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  public ADDFManager() {
    this.initialize(null);
  }

  public ADDFManager(DDF theDDF) {
    this.initialize(theDDF);
  }

  private void initialize(DDF theDDF) {
    this.setDDF(theDDF);
    PhantomReference.register(this);
  }


  /**
   * Returns the DDF engine name of a particular implementation, e.g., "spark"
   * 
   * @return
   */
  public abstract String getDDFEngine();

  protected String getConfigValue(String key) {
    Config.Section section = DDF.getConfig().get(this.getDDFEngine());
    return section == null ? null : section.get(key);
  }


  private DDF mDDF;

  /**
   * 
   * @return the DDF that I directly manage, if any
   */
  public DDF getDDF() {
    return mDDF;
  }

  /**
   * Sets the DDF that I should directly manage
   * 
   * @param aDDF
   * @return
   */
  public ADDFManager setDDF(DDF aDDF) {
    this.mDDF = aDDF;
    return this;
  }


  /**
   * Instantiate a new DDF with the given name, and populates it with the given columns
   * 
   * @param name
   * @param columnsWithData
   * @return
   * @throws DDFException
   */
  public DDF newDDF(String name, ColumnWithData[] columnsWithData) throws DDFException {
    DDF newDDF = this.newDDF();
    ADDFManager manager = newDDF.getManager();

    // Set the new DDF's schema properly
    Schema schema = new Schema(name, columnsWithData);
    manager.getSchemaHandler().setSchema(schema);

    // Now populate the new DDF with data
    // TODO

    return newDDF;
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF". Simultaneously, also
   * instantiate a new DDFManger of the type specified in ddf.ini as "DDFManager". Then, the two are
   * linked to each other.
   * 
   * @return the newly instantiated DDF
   * 
   * @throws DDFException
   */
  protected DDF newDDF() throws DDFException {
    try {
      DDF ddf = (DDF) Class.forName(this.getConfigValue("DDF")).newInstance();
      if (ddf == null) throw new DDFException("Cannot instantiate a new instance of " + this.getConfigValue("DDF"));

      ADDFManager manager = (ADDFManager) Class.forName(this.getConfigValue("DDFManager")).newInstance();
      if (manager == null) throw new DDFException("Cannot instantiate a new instance of "
          + this.getConfigValue("DDFManager"));

      ddf.setManager(manager);
      manager.setDDF(ddf);

      return ddf;

    } catch (Exception e) {
      throw new DDFException("While instantiating a new DDF", e);
    }
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
    String className = this.getConfigValue(theInterface.getSimpleName());

    try {
      Class<?> clazz = Class.forName(className);
      Constructor<ADDFFunctionalGroupHandler> cons = (Constructor<ADDFFunctionalGroupHandler>) clazz
          .getConstructor(new Class<?>[] { ADDFManager.class });
      return cons != null ? (I) cons.newInstance(this) : null;
    } catch (Exception e) {
      LOG.error(String.format("Cannot instantiate handler for %s/%s", theInterface.getSimpleName(), className), e);
      return null;
    }
  }

  private IComputeBasicStatistics mBasicStatisticsComputer;
  private IHandleFactors mFactorHandler;
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
  private AFactorHandler mFactorSupporter;

  public IComputeBasicStatistics getBasicStatisticsComputer() {
    if (mBasicStatisticsComputer == null) mBasicStatisticsComputer = this.createBasicStatisticsComputer();
    if (mBasicStatisticsComputer == null) throw new UnsupportedOperationException();
    else return mBasicStatisticsComputer;
  }

  public ADDFManager setBasicStatisticsComputer(IComputeBasicStatistics aBasicStatisticsComputer) {
    this.mBasicStatisticsComputer = aBasicStatisticsComputer;
    return this;
  }

  protected IComputeBasicStatistics createBasicStatisticsComputer() {
    return newHandler(IComputeBasicStatistics.class);
  }


  public IHandleFactors getFactorHandler() {
    if (mFactorHandler == null) mFactorHandler = this.createFactorHandler();
    if (mFactorHandler == null) throw new UnsupportedOperationException();
    else return mFactorHandler;
  }

  public ADDFManager setFactorHandler(IHandleFactors aFactorHandler) {
    this.mFactorHandler = aFactorHandler;
    return this;
  }

  protected IHandleFactors createFactorHandler() {
    return newHandler(IHandleFactors.class);
  }


  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) mIndexingHandler = this.createIndexingHandler();
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public ADDFManager setIndexingHandler(IHandleIndexing anIndexingHandler) {
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

  public ADDFManager setJoinsHandler(IHandleJoins aJoinsHandler) {
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

  public ADDFManager setMetaDataHandler(IHandleMetaData aMetaDataHandler) {
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

  public ADDFManager setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
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

  public ADDFManager setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
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

  public ADDFManager setMutabilityHandler(IHandleMutability aMutabilityHandler) {
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

  public ADDFManager setSqlHandler(IHandleSql ASqlHandler) {
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

  public ADDFManager setRepresentationHandler(IHandleRepresentations aRepresentationHandler) {
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

  public ADDFManager setReshapingHandler(IHandleReshaping aReshapingHandler) {
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

  public ADDFManager setSchemaHandler(IHandleSchema aSchemaHandler) {
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

  public ADDFManager setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
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

  public ADDFManager setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
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

  public ADDFManager setViewHandler(IHandleViews aViewHandler) {
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

  public ADDFManager setAlgorithmRunner(IRunAlgorithms aAlgorithmRunner) {
    this.mAlgorithmRunner = aAlgorithmRunner;
    return this;
  }

  protected IRunAlgorithms createAlgorithmRunner() {
    return newHandler(IRunAlgorithms.class);
  }

  public AFactorHandler getFactorSupporter() {
    // IMPLEMENTATION HERE
    return mFactorSupporter;
  }

  // ////// IHandleSql ////////

  @Override
  public DDF sql2ddf(String command) throws DDFException {
    return this.getSqlHandler().sql2ddf(command);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.getSqlHandler().sql2ddf(command, schema);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.getSqlHandler().sql2ddf(command, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.getSqlHandler().sql2ddf(command, schema, dataSource);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.getSqlHandler().sql2ddf(command, schema, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    return this.getSqlHandler().sql2ddf(command, schema, dataSource, dataFormat);
  }


  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.getSqlHandler().sql2txt(command);
  }

  @Override
  public List<String> sql2txt(String command, String dataSource) throws DDFException {
    return this.getSqlHandler().sql2txt(command, dataSource);
  }



  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was
   * registered under {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    // @formatter:off
    this
    .setDDF(null)
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
    .shutdown()
    ;
    // @formatter:on
  }

}
