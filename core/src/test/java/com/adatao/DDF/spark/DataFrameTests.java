/**
 * 
 */
package com.adatao.DDF.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.adatao.DDF.DataFrame;

/**
 * @author ctn
 * 
 */
public class DataFrameTests {
  private static JavaSparkContext sc;
  private static List<String> list = new ArrayList<String>();


  @BeforeClass
  public static void setupFixture() {
    String sparkHome = System.getenv("SPARK_HOME");
    if (sparkHome == null) sparkHome = "/root/spark";

    sc = new JavaSparkContext("local", DataFrameTests.class.getName(), sparkHome, (String) null);

    list.add("a");
    list.add("b");
    list.add("c");
  }
  
  @AfterClass
  public static void shutdownFixture() {
    sc.stop();
  }


  @Test
  public void testCreateDataFrame() {
    DataFrame newInstance = DataFrameImplementor.newDataFrame();
    Assert.assertNotNull("Newly instantiated DataFrame should not be null", newInstance);

    newInstance = DataFrameImplementor.newDataFrame(sc.parallelize(list, 1).rdd());
    Assert.assertNotNull("Newly instantiated DataFrame from RDD should not be null", newInstance);
  }
}
