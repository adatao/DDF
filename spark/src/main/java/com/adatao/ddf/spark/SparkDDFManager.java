package com.adatao.ddf.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

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
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.etl.IHandlePersistence;
import com.adatao.ddf.etl.IHandleReshaping;
import com.adatao.ddf.exception.DDFException;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 * 
 */
public class SparkDDFManager extends ADDFManager {
  private JavaSparkContext mSparkContext;

  public SparkDDFManager(String masterUri) throws DDFException {
    this(masterUri, null);
  }

  public SparkDDFManager(String masterUri, Map<String, String> params) throws DDFException {
    this.mSparkContext = this.getSparkContext(masterUri, params);
  }



  public void shutdown() {
    if (mSparkContext != null) {
      mSparkContext.stop();
    }
  }

  private JavaSparkContext getSparkContext(String masterUri) throws DDFException {
    Map<String, String> env = System.getenv();
    Map<String, String> params = new HashMap<String, String>();
    params.put("spark.home", env.get("SPARK_HOME"));
    params.put("DDFSPARK_JAR", env.get("DDFSPARK_JAR"));

    return this.getSparkContext(masterUri, params);
  }

  private JavaSparkContext getSparkContext(String masterUri, Map<String, String> params) throws DDFException {
    try {
      if (params == null) return this.getSparkContext(masterUri);

      String[] jobJars = params.get("DDFSPARK_JAR").split(",");
      return new JavaSparkContext(masterUri, "DDFClient", params.get("SPARK_HOME"), jobJars, params);
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }

  @Override
  public DDF load(String command) {
    // TODO Auto-generated method stub
    return null;
  }



  @Override
  public DDF load(String command, String schema) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, String schema, String source) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, String schema, String source, DataFormat format) {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
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

}
