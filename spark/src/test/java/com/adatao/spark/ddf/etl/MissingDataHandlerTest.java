package com.adatao.spark.ddf.etl;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.etl.IHandleMissingData.Axis;
import com.adatao.ddf.etl.IHandleMissingData.NAChecking;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.types.AggregateTypes.AggregateFunction;

public class MissingDataHandlerTest {
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

    manager.sql2txt("load data local inpath '../resources/test/airlineWithNA.csv' into table airline");
    ddf = manager.sql2ddf("select * from airline");
  }

  @Test
  public void testDropNA() throws DDFException {
    DDF newddf = ddf.dropNA();
    Assert.assertEquals(9, newddf.getNumRows());
    Assert.assertEquals(22, ddf.getMissingDataHandler().dropNA(Axis.COLUMN, NAChecking.ANY, 0, null).getNumColumns());
    
    Assert.assertEquals(29, ddf.getMissingDataHandler().dropNA(Axis.COLUMN, NAChecking.ALL, 0, null).getNumColumns());
  }

  @Test
  @Ignore
  public void testFillNA() throws DDFException {
    DDF ddf1 = ddf.Views.project(Arrays.asList("year", "origin", "securitydelay", "lateaircraftdelay"));

    // test fill by scalar value
    DDF newddf = ddf1.fillNA("0");
    Assert.assertEquals(282, newddf.aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);
    Assert.assertEquals(301, ddf1.fillNA("1").aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);

    // test fill by aggregate function
    ddf1.getMissingDataHandler().fillNA(null, null, 0, AggregateFunction.MEAN, null, null);

    // test fill by dictionary, with mutable DDF
    Map<String, String> dict = new HashMap<String, String>() {
      {
        put("year", "2000");
        put("securitydelay", "0");
        put("lateaircraftdelay", "1");
      }
    };

    ddf1.setMutable(true);
    ddf1.getMissingDataHandler().fillNA(null, null, 0, null, dict, null);
    Assert.assertEquals(301, ddf1.aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);
  }

  @After
  public void closeTest() {
    manager.shutdown();
  }

}
