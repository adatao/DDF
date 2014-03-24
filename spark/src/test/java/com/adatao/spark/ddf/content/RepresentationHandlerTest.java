package com.adatao.spark.ddf.content;


import java.lang.reflect.Array;
import junit.framework.Assert;
import org.apache.spark.rdd.RDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import shark.api.Row;
import shark.memstore2.TablePartition;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;

public class RepresentationHandlerTest {
  private DDFManager manager;
  private DDF ddf;
  private Object data;



  @Before
  public void setUp() throws Exception {
    manager = DDFManager.get("spark");
    ddf = manager.sql2ddf("select year, month, dayofweek, deptime from airline");
    data = ddf.getRepresentationHandler().getDefault();
    // data = (RDD<Row>)ddf.getRDD(Row.class);
    System.out.println(data.getClass());
  }

  @Test
  @Ignore
  public void testDefaultRepresentation() throws DDFException {

    Assert.assertEquals(ddf.getRepresentationHandler().getSpecsAsString(new Class<?>[] { RDD.class, Row.class }), ddf
        .getRepresentationHandler().getSpecsAsString(new Class<?>[] { data.getClass() }));
  }

  @SuppressWarnings("unchecked")
  @Test
  @Ignore
  public void testRowToTablePartion() throws DDFException {

    RDD<TablePartition> rddTablePartition = (RDD<TablePartition>) ddf.getRepresentationHandler().get(RepresentationHandler.RDD_TABLE_PARTITION());
    Assert.assertEquals(
        ddf.getRepresentationHandler().getSpecsAsString(new Class<?>[] { RDD.class, TablePartition.class }), ddf
            .getRepresentationHandler().getSpecsAsString(new Class<?>[] { rddTablePartition.getClass() }));
  }

/*
 * Failed tests: 
  RepresentationHandlerTest.testRowToArrayDouble:58 null expected:<...rg.apache.spark.rdd.[RDD:java.lang.reflect.Array:java.lang.Double]> but was:<...rg.apache.spark.rdd.[MappedRDD]>
 */
  @SuppressWarnings("unchecked")
  @Test
  public void testRowToArrayDouble() throws DDFException {

    RDD<Double[]> rddArrayDouble = (RDD<Double[]>) ddf.getRepresentationHandler().get(RepresentationHandler.RDD_ARRAY_DOUBLE());

    Assert.assertEquals(
        ddf.getRepresentationHandler().getSpecsAsString(new Class<?>[] { RDD.class, Array.class, Double.class }), ddf
            .getRepresentationHandler().getSpecsAsString(new Class<?>[] { rddArrayDouble.getClass() }));
  }

  @After
  public void closeTest() {
    manager.shutdown();
  }


}
