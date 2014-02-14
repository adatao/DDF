package com.adatao.ddf.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;
import shark.api.Row;
import shark.memstore2.TablePartition;
import shark.api.ColumnDesc;

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
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.spark.content.MetaDataHandler;
import com.google.common.collect.Lists;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 * 
 */
public class SparkDDFManager extends ADDFManager {
  private static final String DEFAULT_SPARK_APPNAME = "DDFClient";


  private JavaSharkContext mSharkContext;

  public JavaSharkContext getSparkContext() {
    return mSharkContext;
  }

  private void setSparkContext(JavaSharkContext sparkContext) {
    this.mSharkContext = sparkContext;
  }


  private Map<String, String> mSharkContextParams;

  public Map<String, String> getSparkContextParams() {
    return mSharkContextParams;
  }

  private void setSparkContextParams(Map<String, String> mSharkContextParams) {
    this.mSharkContextParams = mSharkContextParams;
  }



  public SparkDDFManager(JavaSharkContext sparkContext) throws DDFException {
    this.initialize(sparkContext, null);
  }

  public SparkDDFManager() throws DDFException {
    this.initialize(null, null);
  }

  public SparkDDFManager(Map<String, String> params) throws DDFException {
    this.initialize(null, params);
  }

  private void initialize(JavaSharkContext sparkContext, Map<String, String> params) throws DDFException {
    this.mSharkContext = (sparkContext == null ? this.createSparkContext(params) : sparkContext);
  }

  public void shutdown() {
    if (mSharkContext != null) {
      mSharkContext.stop();
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
   * Side effect: also sets SparkContext and SparkContextParams in case the client wants to examine
   * or use those.
   * 
   * @param params
   * @return
   * @throws DDFException
   */
  private JavaSharkContext createSparkContext(Map<String, String> params) throws DDFException {
    try {
      this.setSparkContextParams(this.mergeSparkParamsFromSettings(params));
      String[] jobJars = params.get("DDFSPARK_JAR").split(",");

      this.setSparkContext(new JavaSharkContext(params.get("SPARK_MASTER"), params.get("SPARK_APPNAME"), params
          .get("SPARK_HOME"), jobJars, params));

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
    return new MetaDataHandler(this);
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleReshaping createReshapingHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected IHandleSchema createSchemaHandler() {
    // TODO Auto-generated method stub
    return null;
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
  public DDF load(String tableName) {
    String cmd = String.format("SELECT * FROM %s", tableName);
    JavaTableRDD tableRdd = mSharkContext.sql2rdd(cmd);
    ColumnDesc[] fs = tableRdd.schema();
    Schema theSchema = this.createMetaDataHandler().getSchema();
    List<Column> cols = Lists.newArrayList();
    for (int i = 0; i < fs.length; i++) {
      cols.add(new Column(fs[i].columnName(), fs[i].typeName());
    }
    return new SparkDDF(tableRdd.map(new TablePartitionMapper()).rdd(), new Schema(tableName, cols));
  }

  @Override
  public DDF load(String command, Schema schema) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, DataFormat dataFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, DataFormat dataFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource, DataFormat dataFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  static public class TablePartitionMapper extends Function<Row, TablePartition> {

    public TablePartitionMapper() {
      super();
    }

    @Override
    public TablePartition call(Row t) throws Exception {
      return  (TablePartition) t.rawdata();
    }
  }
}
