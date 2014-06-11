package com.adatao.spark.ddf;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import com.adatao.ddf.exception.DDFException;

public class HBaseDDFManagerTests {

  private static Configuration conf = null;
  /**
   * Initialization
   */
  static {
    conf = HBaseConfiguration.create();
  }


  /**
   * Create a table
   */
  public static void creatTable(String tableName, String[] familys) throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (admin.tableExists(tableName)) {
      System.out.println("table already exists!");
    } else {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      for (int i = 0; i < familys.length; i++) {
        tableDesc.addFamily(new HColumnDescriptor(familys[i]));
      }
      admin.createTable(tableDesc);
      System.out.println("create table " + tableName + " ok.");
    }
  }

  /**
   * Delete a table
   */
  public static void deleteTable(String tableName) throws Exception {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      System.out.println("delete table " + tableName + " ok.");
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    }
  }

  /**
   * Put (or insert) a row
   */
  public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value)
      throws Exception {
    try {
      HTable table = new HTable(conf, tableName);
      Put put = new Put(Bytes.toBytes(rowKey));
      put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
      table.put(put);
      System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Delete a row
   */
  public static void delRecord(String tableName, String rowKey) throws IOException {
    HTable table = new HTable(conf, tableName);
    List<Delete> list = new ArrayList<Delete>();
    Delete del = new Delete(rowKey.getBytes());
    list.add(del);
    table.delete(list);
    System.out.println("del recored " + rowKey + " ok.");
  }

  /**
   * Get a row
   */
  public static void getOneRecord(String tableName, String rowKey) throws IOException {
    HTable table = new HTable(conf, tableName);
    Get get = new Get(rowKey.getBytes());
    Result rs = table.get(get);
    for (KeyValue kv : rs.raw()) {
      System.out.print(new String(kv.getRow()) + " ");
      System.out.print(new String(kv.getFamily()) + ":");
      System.out.print(new String(kv.getQualifier()) + " ");
      System.out.print(kv.getTimestamp() + " ");
      System.out.println(new String(kv.getValue()));
    }
  }

  /**
   * Scan (or list) a table
   */
  public static void getAllRecord(String tableName) {
    try {
      HTable table = new HTable(conf, tableName);
      Scan s = new Scan();
      ResultScanner ss = table.getScanner(s);
      for (Result r : ss) {
        for (KeyValue kv : r.raw()) {
          System.out.print(new String(kv.getRow()) + " ");
          System.out.print(new String(kv.getFamily()) + ":");
          System.out.print(new String(kv.getQualifier()) + " ");
          System.out.print(kv.getTimestamp() + " ");
          System.out.println(new String(kv.getValue()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /*
   * helpfer function to create and import csv file to hbase table
   */
  public static void importHbaseTable(String csvFile) {
    
  }
  
  private DDFManager manager;


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
    
  }

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    try {
      SparkDDFManager manager = (SparkDDFManager) DDFManager.get("spark");
      Assert.assertEquals("spark", manager.getEngine());
//      HBaseDDFManager hb = (HBaseDDFManager) manager;
      DDF ddf = manager.loadTable("airport", Arrays.asList("cf:ap", "cf:city", "cf:id", "cf:lat", "cf:lon", "cf:state"));

//      long nrow = ddf.getNumRows();
//      System.out.println(">>>>>>>>>>> numrows = " + nrow);
      List<List<DDF>> lstDDFs = ddf.ML.CVKFold(3, (long) 1);
      java.util.Iterator<List<DDF>> it = lstDDFs.iterator();
      while(it.hasNext()) {
        List<DDF> a = it.next();
        System.out.println(">>>>>>>>>>>>> a.size=" + a.size());
      }
      
      //load another ddf from Shark
      DDF sharkDdf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
//      ddf.getJoinsHandler().
      
//      ddf.ML.linearRegressionNQ(1, 0.1);
      
//      List<String> b = ddf.getSqlHandler().sql2txt("select * from test2");
//    System.out.println(">>>>>:b=" + b.get(0));
    
//      FiveNumSummary[] fnum = ddf.getFiveNumSummary();

//      String tablename = "scores";
//      String[] familys = { "grade", "course" };
//      HBaseDDFManagerTests.creatTable(tablename, familys);
//
//      System.out.println(">>>>>>>> after create table");
//
//      // add record zkb
//      HBaseDDFManagerTests.addRecord(tablename, "zkb", "grade", "", "5");
//      HBaseDDFManagerTests.addRecord(tablename, "zkb", "course", "", "90");
//      HBaseDDFManagerTests.addRecord(tablename, "zkb", "course", "math", "97");
//      HBaseDDFManagerTests.addRecord(tablename, "zkb", "course", "art", "87");
//      // add record baoniu
//      HBaseDDFManagerTests.addRecord(tablename, "baoniu", "grade", "", "4");
//      HBaseDDFManagerTests.addRecord(tablename, "baoniu", "course", "math", "89");
//
//      System.out.println("===========get one record========");
//      HBaseDDFManagerTests.getOneRecord(tablename, "zkb");
//
//      System.out.println("===========show all record========");
//      HBaseDDFManagerTests.getAllRecord(tablename);
//
//      System.out.println("===========del one record========");
//      HBaseDDFManagerTests.delRecord(tablename, "baoniu");
//      HBaseDDFManagerTests.getAllRecord(tablename);
//
//      System.out.println("===========show all record========");
//      HBaseDDFManagerTests.getAllRecord(tablename);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
