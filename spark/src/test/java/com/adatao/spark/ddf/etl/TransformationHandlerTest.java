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
    DDF newddf0 = ddf.Transform.transformScaleMinMax();
    Summary[] summaryArr = newddf0.getSummary();
    Assert.assertTrue(summaryArr[0].min() < 1);
    Assert.assertTrue(summaryArr[0].max() == 1);

  }

  @Test
  @Ignore 
  public void testTransformScaleStandard() throws DDFException {
    DDF newddf1 = ddf.Transform.transformScaleStandard();
    Assert.assertEquals(31, newddf1.getNumRows());
    Assert.assertEquals(8, newddf1.getSummary().length);
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
    
//    System.out.println(">>>>>>>>>>>>>>>>>>>> FIRST ROW 1" +ddf.Views.firstNRows(1).get(0));
    ddf.setMutable(true);
    ddf = ddf.Transform.transformUDF("dist= round(distance/2, 2)");
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(9, ddf.getNumColumns());
    Assert.assertEquals("dist", ddf.getColumnName(8));
    
    
//    Assert.assertEquals(9, ddf.Views.firstNRows(1).get(0).split("\\t").length);
    ddf.getRepresentationHandler().getDefault();
    System.out.println(">>>>>>>>>>>>>>>>>>>> after transform FIRST ROW 1" +ddf.Views.firstNRows(1).get(0));
    
    //Assert.assertEquals(9, ddf.getSummary().length);

  //specifying selected column list
    List<String> cols = Lists.newArrayList("year","month","dayofweek");
    
    System.out.println(">>>>>>>>>>>>> before run second transform");
    System.out.println(">>>>>>>>>>>" + ddf.getManager().sql2txt("select * from "+ ddf.getTableName()));
    
    System.out.println(">>>>>>>>>>> sql2ddf =" + ddf.getManager().sql2ddf("select * from "+ ddf.getTableName()));
    
    ddf = ddf.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(4, ddf.getNumColumns());
    Assert.assertEquals("speed", ddf.getColumnName(3));
    System.out.println(">>>>>>>>>>>>>>>>>>>> FIRST ROW 2" +ddf.Views.firstNRows(1).get(0));
    
/*    DDF ddf0 = ddf.Transform.transformUDF("dist= round(distance/2, 2)");
    Assert.assertEquals(31, ddf0.getNumRows());
    Assert.assertEquals(9, ddf0.getNumColumns());
    Assert.assertEquals("dist", ddf0.getColumnName(8));
    
    //specifying selected column list
    List<String> cols = Lists.newArrayList("year","month","dayofweek");
    
    DDF ddf1 = ddf.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf1.getNumRows());
    Assert.assertEquals(4, ddf1.getNumColumns());
    Assert.assertEquals("speed", ddf1.getColumnName(3));
    
    //udf without assigning column name
    DDF ddf2 = ddf.Transform.transformUDF("arrtime-deptime");
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(9, ddf2.getNumColumns());
    
    //multiple expressions, column name with special characters
    DDF ddf3 = ddf0.Transform.transformUDF("arrtime-deptime, (speed^*- = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf3.getNumRows());
    Assert.assertEquals(5, ddf3.getNumColumns());
    Assert.assertEquals("speed", ddf3.getColumnName(4));*/
  }
  
  @After
  public void closeTest() {
    manager.shutdown();
  }

}
