package com.adatao.spark.ddf.etl;


import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.content.Schema.ColumnType;
import com.adatao.ddf.exception.DDFException;
import com.google.common.collect.Lists;

public class TransformationHandlerTest {
  private DDFManager manager;
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    manager = DDFManager.get("spark");
    manager.sql2txt("drop table if exists airline");

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/airline.csv' into table airline");

    ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
  }

  @Test
  @Ignore
  public void testTransformNativeRserve() throws DDFException {

    DDF newddf = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime");
    System.out.println("name " + ddf.getName());
    System.out.println("newname " + newddf.getName());
    List<String> res = ddf.Views.firstNRows(10);
    Assert.assertNotNull(newddf);
    Assert.assertEquals("newcol", newddf.getColumnName(9));
    Assert.assertEquals(10, res.size());
  }

  @Test
  @Ignore
  public void testTransformScaleMinMax() throws DDFException {

    DDF newddf = ddf.Transform.transformScaleMinMax();
    Summary[] summaryArr = newddf.getSummary();
    Assert.assertTrue(summaryArr[0].min() < 1);
    Assert.assertTrue(summaryArr[0].max() == 1);

  }

  @Test
  @Ignore
  public void testTransformScaleStandard() throws DDFException {

/*    DDF newddf = ddf.Transform.transformScaleStandard();
    Assert.assertNotNull(newddf);

    Assert.assertTrue(ddf.getSchema().getTableName().equals(newddf.getSchema().getTableName()));
    Assert.assertTrue(ddf.getName().equals(newddf.getName()));
    Assert.assertTrue(ddf.getNumRows() == newddf.getNumRows());*/
    
    ddf = ddf.Transform.transformScaleStandard();
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(8, ddf.getSummary().length);
  }

  @Test
  @Ignore
  public void testTransformMapReduceNative() throws DDFException {

    // aggregate sum of month group by year

    String mapFuncDef = "function(part) { keyval(key=part$year, val=part$month) }";
    String reduceFuncDef = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }";
    DDF newddf = ddf.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef);
    System.out.println("name " + ddf.getName());
    System.out.println("newname " + newddf.getName());
    Assert.assertNotNull(newddf);
    Assert.assertTrue(newddf.getColumnName(0).equals("key"));
    Assert.assertTrue(newddf.getColumnName(1).equals("val"));

    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(0).getType() == ColumnType.STRING);
    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(1).getType() == ColumnType.INT);
  }
  
  
  @Test
  
  public void testTransformSql() throws DDFException {
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(8, ddf.getNumColumns());
    
    //List<String> cols = Lists.newArrayList("year","month","dayofweek");
    DDF ddf1 = ddf.Transform.transformUDF("speed = distance/(arrtime-deptime)");
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(4, ddf.getNumColumns());
    Assert.assertEquals("speed", ddf.getColumnName(3));

  }
  @After
  public void closeTest() {
    manager.shutdown();
  }

}
