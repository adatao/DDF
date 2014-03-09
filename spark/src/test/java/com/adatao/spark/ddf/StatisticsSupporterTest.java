package com.adatao.spark.ddf;


import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;


public class StatisticsSupporterTest {

  @Test
  public void testSimpleAggregate() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));
    /*manager.sql2txt("drop table if exists airlinetest");

    manager.sql2txt("create table airlinetest (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/airline.csv' into table airlinetest");*/
    DDF ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
    Assert.assertEquals(14, ddf.getSummary().length);
    
    //aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    Assert.assertEquals(13, ddf.aggregate("year, month, min(depdelay), max(arrdelay)").size());
    Assert.assertEquals(2, ddf.aggregate("year, month, min(depdelay), max(arrdelay)").get("2010,3").length);
    
    //project subset
    // Assert.assertEquals(3, ddf.Views.project(new String[]{"year", "month", "deptime"}).getNumColumns());
    manager.shutdown();
  }

}
