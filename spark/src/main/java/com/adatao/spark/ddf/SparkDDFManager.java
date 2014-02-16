package com.adatao.spark.ddf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;

import shark.SharkContext;
import shark.SharkEnv;
import shark.api.JavaSharkContext;

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
import com.adatao.ddf.etl.IHandleDataCommands;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.analytics.SparkAlgorithmRunner;
import com.adatao.spark.ddf.content.SparkMetaDataHandler;
import com.adatao.spark.ddf.content.SparkSchemaHandler;
import com.adatao.spark.ddf.content.SparkRepresentationHandler;
import com.adatao.spark.ddf.etl.SparkDataCommandHandler;

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

  public SharkContext getSharkContext() {
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
    { "SPARK_SERIALIZER", "spark.serializer" },
    { "HIVE_HOME", "hive.home" },
    { "HADOOP_HOME", "hadoop.home" },
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
      this.setSharkContext(SharkEnv.initWithJavaSharkContext(jsc).sharkCtx());

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
    return new SparkMetaDataHandler(this);
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
  protected IHandleDataCommands createDataCommandHandler() {
    return new SparkDataCommandHandler(this);
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
    return new SparkAlgorithmRunner();
  }



  // ////// IHandleDataCommands ////////

  @Override
  public DDF cmd2ddf(String command) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command, schema);
  }

  @Override
  public DDF cmd2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command, dataFormat);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command, schema, dataSource);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command, schema, dataFormat);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    return this.getDataCommandHandler().cmd2ddf(command, schema, dataSource, dataFormat);
  }


  @Override
  public List<String> cmd2txt(String command) throws DDFException {
    return this.getDataCommandHandler().cmd2txt(command);
  }

  @Override
  public List<String> cmd2txt(String command, String dataSource) throws DDFException {
    return this.getDataCommandHandler().cmd2txt(command, dataSource);
  }
  
}
