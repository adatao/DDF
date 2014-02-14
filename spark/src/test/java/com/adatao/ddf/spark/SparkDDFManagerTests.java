package com.adatao.ddf.spark;


import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.adatao.ddf.spark.SparkDDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.DDF;

public class SparkDDFManagerTests {

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    Map<String, String> env = System.getenv();
    String masterUri = env.get("SPARK_MASTER");

    Map<String, String> params = new HashMap<String, String>();
    params.put("spark.home", System.getProperty("SPARK_HOME"));
    params.put("DDFSPARK_JAR", env.get("DDFSPARK_JAR"));
    params.put("SPARK_MASTER", env.get("SPARK_MASTER"));
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));

    SparkDDFManager ddfManager = new SparkDDFManager(masterUri, params);
    System.out.println(ddfManager);

    ddfManager.shutdown();
  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {
    Map<String, String> env = System.getenv();
    String masterUri = env.get("SPARK_MASTER");
    SparkDDFManager ddfManager = new SparkDDFManager(masterUri);
    System.out.println(ddfManager);

    // Now you can create DDF
    DDF ddf = ddfManager.load("select * from airline");
    System.out.println(ddf);

    ddfManager.shutdown();
  }

}
