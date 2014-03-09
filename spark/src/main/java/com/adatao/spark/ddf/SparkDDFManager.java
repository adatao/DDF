package com.adatao.spark.ddf;


import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;

import shark.SharkContext;
import shark.SharkEnv;
import shark.api.JavaSharkContext;

import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;

/**
 * An Apache-Spark-based implementation of DDFManager
 */
public class SparkDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "spark";
  }


  private static final String DEFAULT_SPARK_APPNAME = "DDFClient";
  private static final String DEFAULT_SPARK_MASTER = "local[4]";



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



  public String getDDFEngine() {
    return "spark";
  }


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
    { "DDFLIB_JAR", "ddfspark.jar" },
    { "DDFSPARK_JAR", "ddfspark.jar" } 
    // @formatter:on
  };


  /**
   * Takes an existing params map, and reads both environment as well as system property settings to merge into it. The
   * merge priority is as follows: (1) already set in params, (2) in system properties (e.g., -Dspark.home=xxx), (3) in
   * environment variables (e.g., export SPARK_HOME=xxx)
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

    // Some well-known defaults
    if (!params.containsKey("SPARK_MASTER")) params.put("SPARK_MASTER", DEFAULT_SPARK_MASTER);
    if (!params.containsKey("SPARK_APPNAME")) params.put("SPARK_APPNAME", DEFAULT_SPARK_APPNAME);

    return params;
  }

  /**
   * Side effect: also sets SharkContext and SparkContextParams in case the client wants to examine or use those.
   * 
   * @param params
   * @return
   * @throws DDFException
   */
  private SparkContext createSparkContext(Map<String, String> params) throws DDFException {
    this.setSparkContextParams(this.mergeSparkParamsFromSettings(params));
    String ddfSparkJar = params.get("DDFSPARK_JAR") + "," +  listJarFiles(params.get("DDFLIB_JAR"));
    String[] jobJars = ddfSparkJar != null ? ddfSparkJar.split(",") : new String[] {};

    JavaSharkContext jsc = new JavaSharkContext(params.get("SPARK_MASTER"), params.get("SPARK_APPNAME"),
        params.get("SPARK_HOME"), jobJars, params);
    this.setSharkContext(SharkEnv.initWithJavaSharkContext(jsc).sharkCtx());


    return this.getSparkContext();
  }
  
  private String listJarFiles(String path) {
    File folder = new File(path);
    
    List<String> jarFiles = new ArrayList<String>();
    for (File f: folder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        // TODO Auto-generated method stub
        return pathname.getName().endsWith(".jar");
      }
    })) {
      try {
        jarFiles.add(f.getCanonicalPath());
      } catch (IOException ioe) {
        
      }
    }
    
    return StringUtils.join(jarFiles, ",");
  }
}
