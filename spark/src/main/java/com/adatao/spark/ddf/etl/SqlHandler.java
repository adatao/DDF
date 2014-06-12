/**
 * 
 */
package com.adatao.spark.ddf.etl;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Option;
//import com.adatao.spark.ddf.Tuple2;
import scala.Tuple2;
import scala.collection.Seq;
import shark.SharkContext;
import shark.SharkEnv;
import shark.api.Row;
import shark.api.TableRDD;
import shark.memstore2.MemoryTable;
import shark.memstore2.TablePartition;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.ASqlHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.spark.ddf.content.SchemaHandler;

/**
 * @author ctn
 * 
 */
public class SqlHandler extends ASqlHandler {

  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  private SharkContext getSharkContext() {
    return ((SparkDDFManager) this.getManager()).getSharkContext();
  }

  // ////// IHandleDataCommands ////////

  protected void createTableForDDF(String tableName) throws DDFException {

    try {
      DDF ddf = this.getManager().getDDF(tableName);
      ddf.getRepresentationHandler().get(RDD.class, TablePartition.class);
    } catch (Exception e) {
      throw new DDFException(String.format("Can not create table for DDF %s", tableName), e);
    }
  }

  @Override
  public DDF sql2ddf(String command) throws DDFException {
    return this.sql2ddf(command, null, null, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    TableRDD tableRdd = null;
    RDD<Row> rddRow = null;
    // TODO: handle other dataSources and dataFormats

    String tableName = this.getDDF().getSchemaHandler().newTableName();
    boolean isHBase = false;
    String hbaseColumnsNameType = "";

    /**
     * Make sure that the ddf needed was backed by a table Find tableName that match the pattern
     * "... from tableName ..."
     */
    String ddfTableNameFromQuery;
    Pattern p = Pattern.compile("(?<=.from\\s)([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(command);
    mLog.info(">>>>>>> DDF's TableName from query command = " + command);
    if (m.find()) {
      ddfTableNameFromQuery = m.group();
      mLog.info(">>>>>>> DDF's TableName from query = " + ddfTableNameFromQuery);
      if (this.getManager().getDDF(ddfTableNameFromQuery) != null) {
        this.createTableForDDF(ddfTableNameFromQuery);
      }
    }

    if (dataSource == null) {
      String sqlCmd;

      sqlCmd = String.format("CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"MEMORY_ONLY\") AS %s", tableName,
          command);
      this.getManager().sql2txt(sqlCmd);

      tableRdd = this.getSharkContext().sql2rdd(String.format("SELECT * FROM %s", tableName));
      rddRow = (RDD<Row>) tableRdd;
    }
    // for hbase
    else if (dataSource.equals("hbase")) {
      isHBase = true;
      //parse sql to List<String> columns
      //TODO create sqlparser layer here
      List<String> columns = parseHbaseQuery(command.trim());
      String hbTableName = parseHbaseTable(command).trim();
      SparkDDFManager currentManager = (SparkDDFManager)this.getManager();
      return (currentManager.loadTable(hbTableName, columns));
//      // set SCAN in conf
//      try {
//        config.set(TableInputFormat.SCAN, convertScanToString(scan));
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//      RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = currentManager.getSparkContext().newAPIHadoopRDD(config,
//          TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//      RDD<Object[]> result = convertRDD2(hBaseRDD);
//      
////      long a = result.count();
////      System.out.println(">>>> count table = " + a);
//      
//      currentManager.sql2txt("drop table if exists " + hbTableName);
//      Schema schema2 = new Schema(hbTableName, hbaseColumnsNameType); 
//      DDF ddf =  new SparkDDF(currentManager, result, Object[].class, "", hbTableName, schema2);
//      ddf.getRepresentationHandler().get(RDD.class, TablePartition.class);
//      return ddf;
    }
    // TODO
    else {}

      if (schema == null) schema = SchemaHandler.getSchemaFrom(tableRdd.schema());
      /*
       * String tableName = (schema != null ? schema.getTableName() : null);
       * 
       * if (Strings.isNullOrEmpty(tableName)) tableName = (rdd != null ? rdd.name() : null); if
       * (Strings.isNullOrEmpty(tableName)) tableName = this.getDDF().getSchemaHandler().newTableName(); i
       */
      if (tableName != null) {
        schema.setTableName(tableName);
      }
      DDF ddf = new SparkDDF(this.getManager(), rddRow, Row.class, null, tableName, schema);
      try {
        String databaseName = Hive.get(SharkContext.hiveconf()).getCurrentDatabase();
        Option memTableOrNull = SharkEnv.memoryMetadataManager().getMemoryTable(databaseName, tableName);
        if (memTableOrNull.isDefined()) {
          RDD<TablePartition> rddTablePartition = (RDD<TablePartition>) ((MemoryTable) memTableOrNull.get()).getRDD()
              .get();
          ddf.getRepresentationHandler().add(rddTablePartition, RDD.class, TablePartition.class);
        }
      } catch (HiveException e) {
        throw new DDFException(e);
      }
      return ddf;
  }

  private List<String> parseHbaseQuery(String command) {
    command = command.toLowerCase().trim();
    String selectedColumns = command.substring(command.indexOf("select") + 6, command.indexOf("from"));
    String[] c = selectedColumns.split(",");
    ArrayList<String> ret = new  ArrayList<String>();
    for(int i=0; i< c.length; i ++) {
      ret.add(c[i].trim());
    }
    return ret;
  }
  
  private String parseHbaseTable(String command) {
    command = command.toLowerCase();
    String tableName = command.substring(command.indexOf("from") + 4, command.length());
    return tableName;
  }
  

  public RDD<Object[]> convertRDD2(RDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD) {
    RDD<Object[]> result = hbaseRDD.map(new ConvertMapper2(), null);
    return result;
  }


  // GetSummaryMapper extends Function<Object[], Summary[]>

  private static class ConvertMapper2 extends Function<Tuple2<ImmutableBytesWritable, Result>, Object[]> {
    public ConvertMapper2() {
      System.out.println(">>>>>>>>>>>>>> ConvertMapper");
    }

    @Override
    public Object[] call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
      ImmutableBytesWritable key = input._1;
      Result res = input._2;
      List<org.apache.hadoop.hbase.KeyValue> keyValues = res.list();
      int size = keyValues.size();
      Object[] arrResult = new Object[size];
      int i = 0;
      Iterator<KeyValue> it = keyValues.iterator();
      while (it.hasNext()) {
        KeyValue kv = it.next();
        System.out.println(">>> family=" + Bytes.toString(kv.getFamily()));
        System.out.println(">>> qualifier=" + Bytes.toString(kv.getQualifier()));
        String a = Bytes.toString(kv.getValue());
        arrResult[i] = a;
        System.out.println(">>>>>>a=" + a);
        i++;
      }
      return arrResult;
    }
  }


  static String convertScanToString(Scan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    scan.write(dos);
    return Base64.encodeBytes(out.toByteArray());
  }

  private <T> List<T> toList(Seq<T> sequence) {
    return scala.collection.JavaConversions.seqAsJavaList(sequence);
  }


  public static final int MAX_COMMAND_RESULT_ROWS = 1000;


  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.sql2txt(command, null, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows) throws DDFException {
    return this.sql2txt(command, maxRows, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
    // TODO: handle other dataSources
    /**
     * Make sure that the ddf needed was backed by a table Find tableName that match the pattern
     * "... from tableName ..."
     */
    String ddfTableNameFromQuery;
    Pattern p = Pattern.compile("(?<=.from\\s)([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(command);
    mLog.info(">>>>>>> DDF's TableName from query command = " + command);
    if (m.find()) {
      ddfTableNameFromQuery = m.group();
      mLog.info(">>>>>>> DDF's TableName from query = " + ddfTableNameFromQuery);
      if (this.getManager().getDDF(ddfTableNameFromQuery) != null) {
        this.createTableForDDF(ddfTableNameFromQuery);
      }
    }
    return this.toList(getSharkContext().sql(command, maxRows == null ? MAX_COMMAND_RESULT_ROWS : maxRows));
  }
}
