package com.adatao.spark.ddf;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import shark.SharkContext;
import shark.api.JavaSharkContext;
import shark.api.Row;
import shark.api.TableRDD;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.DDF;
import com.adatao.ddf.IHandleMiscellany;
import com.adatao.ddf.IHandleStreamingData;
import com.adatao.ddf.IHandleTimeSeries;
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
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.content.SparkRepresentationHandler;
import com.adatao.spark.ddf.content.SparkSchemaHandler;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 * 
 */
public class SparkDDFManager extends ADDFManager {
  private static final String DEFAULT_SPARK_APPNAME = "DDFClient";


  private SparkContext mSparkContext;

  public SparkContext getSparkContext() {
    return mSparkContext;
  }

  private void setSparkContext(SparkContext sparkContext) {
    this.mSparkContext = sparkContext;
  }


  private SharkContext mSharkContext;

  private SharkContext getSharkContext() {
    return mSharkContext;
  }

  /**
   * Also calls setSparkContext() to the same sharkContext
   * 
   * @param sharkContext
   */
  private void setSharkContext(SharkContext sharkContext) {
    this.mSharkContext = sharkContext;
    this.setSparkContext(sharkContext);
  }

  private Map<String, String> mSparkContextParams;

  public Map<String, String> getSparkContextParams() {
    return mSparkContextParams;
  }

  private void setSparkContextParams(Map<String, String> mSparkContextParams) {
    this.mSparkContextParams = mSparkContextParams;
  }



  public SparkDDFManager(SparkContext sparkContext) throws DDFException {
    this.initialize(sparkContext, null);
  }

  /**
   * Use system environment variables to configure the SparkContext creation.
   * 
   * @throws DDFException
   */
  public SparkDDFManager() throws DDFException {
    this.initialize(null, new HashMap<String, String>());
  }

  public SparkDDFManager(Map<String, String> params) throws DDFException {
    this.initialize(null, params);
  }

  private void initialize(SparkContext sparkContext, Map<String, String> params) throws DDFException {
    this.setSparkContext(sparkContext == null ? this.createSparkContext(params) : sparkContext);

    if (sparkContext instanceof SharkContext) this.setSharkContext((SharkContext) sparkContext);
  }

  public void shutdown() {
    if (this.getSharkContext() != null) {
      this.getSharkContext().stop();
    } else if (this.getSparkContext() != null) {
      this.getSparkContext().stop();
    }
  }


  private static final String[][] SPARK_ENV_VARS = new String[][] { 
    // @formatter:off
    { "SPARK_APPNAME", "spark.appname" },
    { "SPARK_MASTER", "spark.master" }, 
    { "SPARK_HOME", "spark.home" }, 
    { "SPARK_SERIALIZER", "spark.serializer" }, 
    { "DDFSPARK_JAR", "ddfspark.jar" } 
    // @formatter:on
  };

  /**
   * Takes an existing params map, and reads both environment as well as system property settings to
   * merge into it. The merge priority is as follows: (1) already set in params, (2) in system
   * properties (e.g., -Dspark.home=xxx), (3) in environment variables (e.g., export SPARK_HOME=xxx)
   * 
   * @param params
   * @return
   */
  private Map<String, String> mergeSparkParamsFromSettings(Map<String, String> params) {
    if (params == null) params = new HashMap<String, String>();

    Map<String, String> env = System.getenv();

    for (String[] varPair : SPARK_ENV_VARS) {
      if (params.containsKey(varPair[0])) continue; // already set in params

      // Read setting either from System Properties, or environment variable.
      // Env variable has lower priority if both are set.
      String value = System.getProperty(varPair[1], env.get(varPair[0]));
      if (value != null && value.length() > 0) params.put(varPair[0], value);
    }

    // Some miscellaneous stuff specific to us
    if (!params.containsKey("SPARK_APPNAME")) params.put("SPARK_APPNAME", DEFAULT_SPARK_APPNAME);

    return params;
  }

  /**
   * Side effect: also sets SharkContext and SparkContextParams in case the client wants to examine
   * or use those.
   * 
   * @param params
   * @return
   * @throws DDFException
   */
  private SparkContext createSparkContext(Map<String, String> params) throws DDFException {
    try {
      this.setSparkContextParams(this.mergeSparkParamsFromSettings(params));
      String[] jobJars = params.get("DDFSPARK_JAR").split(",");

      JavaSharkContext jsc = new JavaSharkContext(params.get("SPARK_MASTER"), params.get("SPARK_APPNAME"),
          params.get("SPARK_HOME"), jobJars, params);
      this.setSharkContext(jsc.sharkCtx());

    } catch (Exception e) {
      throw new DDFException(e);
    }

    return this.getSparkContext();
  }



  @Override
  protected IComputeBasicStatistics createBasicStatisticsComputer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleIndexing createIndexingHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleJoins createJoinsHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleMetaData createMetaDataHandler() {
    // return new MetaDataHandler(this);
    return null;
  }

  @Override
  protected IHandleMiscellany createMiscellanyHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleMissingData createMissingDataHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleMutability createMutabilityHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandlePersistence createPersistenceHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleRepresentations createRepresentationHandler() {
    return new SparkRepresentationHandler(this);
  }

  @Override
  protected IHandleReshaping createReshapingHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleSchema createSchemaHandler() {
    return new SparkSchemaHandler(this);
  }

  @Override
  protected IHandleStreamingData createStreamingDataHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleTimeSeries createTimeSeriesHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleViews createViewHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IRunAlgorithms createAlgorithmRunner() {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public DDF load(String command) throws DDFException {
    TableRDD tableRdd = this.getSharkContext().sql2rdd(command);
    RDD<Row> rdd = (RDD<Row>) tableRdd;
    Schema schema = SparkSchemaHandler.getSchemaFrom(tableRdd.schema());

    return new SparkDDF(rdd, Row.class, schema);
  }

  @Override
  public DDF load(String command, Schema schema) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

}
