package com.adatao.spark.ddf;


import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.DDF;

public class SparkDDFManagerTests {

  @Test
  @Ignore
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
    ddfManager.cmd2txt("drop table if exists airline");
    ddfManager.cmd2txt("create table airline (Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    // ddfManager.cmd2txt("load data local inpath '/home/cuongbk/Downloads/2008.csv' into table airline");
    ddfManager.cmd2txt("load data local inpath 'src/test/resources/airline.csv' into table airline");
    List<String> results = ddfManager.cmd2txt("select count(*) from airline");
    for (String s: results) {
      System.out.println("DDF test: " + s);
    }
    
    DDF ddf = ddfManager.cmd2ddf("select * from airline");
    Assert.assertEquals(31, ddf.getNumRows()); 
    System.out.println(ddf);
    
    ddfManager.shutdown();
  }

}
