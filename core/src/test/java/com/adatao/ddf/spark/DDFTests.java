/**
 * 
 */
package com.adatao.ddf.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.adatao.ddf.DDF;
import com.adatao.ddf.spark.content.RepresentationHandler;

/**
 * Spark-specific DDF tests
 * 
 * @author ctn
 * 
 */
public class DDFTests {
  private static JavaSparkContext sc;
  private static List<String> list = new ArrayList<String>();


  @BeforeClass
  public static void setupFixture() {
    String sparkHome = System.getenv("SPARK_HOME");
    if (sparkHome == null) sparkHome = "/root/spark";

    sc = new JavaSparkContext("local", DDFTests.class.getName(), sparkHome, (String) null);

    list.add("a");
    list.add("b");
    list.add("c");
  }

  @AfterClass
  public static void shutdownFixture() {
    sc.stop();
  }


  @Test
  public void testCreateDDF() {
    DDF newInstance = DDFHelper.newDDF();
    Assert.assertNotNull("Newly instantiated DDF should not be null", newInstance);

    newInstance = DDFHelper.newDDF(sc.parallelize(list, 1).rdd(), list.get(0).getClass());
    Assert.assertNotNull("Newly instantiated DDF from RDD should not be null", newInstance);
  }

  @Test
  public void testRepresentDDF() {
    DDF newInstance = DDFHelper.newDDF(sc.parallelize(list, 1).rdd(), list.get(0).getClass());
    Assert.assertNotNull("Newly instantiated DDF from RDD should not be null", newInstance);

    Assert.assertTrue(
        "DDF Representation should be RDD[String]",
        ((RepresentationHandler) (newInstance.getHelper().getRepresentationHandler())).getList().startsWith(
            "1. key='class org.apache.spark.rdd.RDD[class java.lang.String]', value='ParallelCollectionRDD"));
  }
}
