package com.adatao.spark.ddf;


import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;

public class SparkDDFManagerTests {

  @Test
  @Ignore
  public void testDDFConfig() throws Exception {
    DDFManager manager = DDFManager.get("spark");
    Assert.assertEquals("spark", manager.getEngine());
  }

  @Test
  @Ignore
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));

    manager.shutdown();
  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {

    DDFManager manager = DDFManager.get("spark");

    /*
     * // Now you can create DDF DDF.sql2txt("drop table if exists airline"); DDF.sql2txt(
     * "create table airline (Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
     * );
     * 
     * DDF.sql2txt("load data inpath '/sampledata/airline.csv' into table airline"); List<String> results =
     * DDF.sql2txt("select count(*) from airline"); for (String s : results) { System.out.println("DDF test: " + s); }
     * 
     * DDF ddf = DDF.sql2ddf("select * from airline"); Assert.assertEquals(31, ddf.getNumRows());
     * System.out.println(ddf);
     */
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

    DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
        + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
    Assert.assertEquals(16, ddf.getSummary().length);
    manager.shutdown();
  }
}
