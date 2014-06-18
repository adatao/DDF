package com.adatao.spark.ddf.etl;

import java.util.Arrays;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
import com.google.common.collect.Lists;

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
  @Ignore
  public void testDropNA() throws DDFException {
    DDF newddf = ddf.dropNA();
    Assert.assertEquals(9, newddf.getNumRows());
    Assert.assertEquals(22, ddf.getMissingDataHandler().dropNA(1, "any", 0, null, false).getNumColumns());
  }
  
  @Test
  public void testFillNA() throws DDFException {
    DDF ddf1 = ddf.Views.project(Arrays.asList("year", "origin", "securitydelay","lateaircraftdelay"));
    DDF newddf = ddf1.fillNA("0");
    //Assert.assertEquals(0, ddf.fillNA("0").aggregate("year, sum(LateAircraftDelay)").get("2008"));
    System.out.println(newddf.Views.firstNRows(2));
  }
  @After
  public void closeTest() {
    manager.shutdown();
  }


}
