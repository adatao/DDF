package com.adatao.spark.ddf;


import java.util.Map;

import org.junit.Test;

import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.DDF;

public class SparkDDFManagerTests {

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    SparkDDFManager ddfManager = new SparkDDFManager();

    Map<String, String> params = ddfManager.getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));

    ddfManager.shutdown();
  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {
    SparkDDFManager ddfManager = new SparkDDFManager();

    // Now you can create DDF
    DDF ddf = ddfManager.load("select * from airline");
    System.out.println(ddf);

    ddfManager.shutdown();
  }

}
