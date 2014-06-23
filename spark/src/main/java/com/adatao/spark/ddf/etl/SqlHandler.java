/**
 * 
 */
package com.adatao.spark.ddf.etl;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.GreaterThanOrEqualParseNode;
import org.apache.phoenix.parse.GreaterThanParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.NotEqualParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.schema.PDataType;
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

    if (dataSource == null || dataSource.equals("") || dataSource.length() < 1) {
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
      // parse sql to List<String> columns
      // TODO create sqlparser layer here
      command = command.trim();
//      String columnTypes = parseColumnTypes(command);
      String hbTableName = parseHbaseTable(command);
//      System.out.println(">>>> khang: " + columnTypes);

      // SparkDDFManager currentManager = (SparkDDFManager)this.getManager();
      SparkDDFManager currentManager = (SparkDDFManager) this.getDDF().getManager();
      DDF ddf = currentManager.loadTable(hbTableName, "", command);
      return (ddf);
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

  /*
   * return Scan object
   */
  public Scan parseQuery(String command) {
    SQLParser parser;
    command = command.replace(":", "_");
    try {
      // make sure input query for SQLParser doesn't contain semi colon
      parser = new SQLParser(new StringReader(command));
      SelectStatement stm = parser.parseQuery();
      Scan scan = new Scan();

      // get table name, assumption select from one table
      // TODO assumption one table
      List<TableNode> lst = stm.getFrom();
      NamedTableNode temp = (NamedTableNode) lst.get(0);
      // tableName = temp.getName().getTableName();

      // get selected columns, just simply list all columns here and add to scan object
      String cf = "";
      String cq = "";
      List<AliasedNode> nodes = stm.getSelect();
      Iterator<AliasedNode> it = nodes.iterator();
      while (it.hasNext()) {
        AliasedNode node = it.next();
        String column = node.getNode().getAlias();
        System.out.println(">>>>>>>>>>>>>>> column  = " + column);
        if (column.contains("_")) {
          cf = column.split("_")[0];
          cq = column.split("_")[1];
          scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq));
        } else {
          cf = column;
          scan.addFamily(Bytes.toBytes(cf));
        }
      }

      // get where conditions here
      ParseNode where = stm.getWhere();
      if (where != null && where.getChildren() != null && where.getChildren().size() > 0) {
        List<ParseNode> children = where.getChildren();
        Iterator<ParseNode> it2 = children.iterator();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        // check what type of where condition
        while (it2.hasNext()) {
          ParseNode child = it2.next();
          list = parseFilter(child, list);
        }
        // set filter
        if (list != null && list.getFilters() != null && list.getFilters().size() > 0) {
          scan.setFilter(list);
        }
      }
      System.out.println(">>>> scan = " + scan.toString());
      return scan;
      // get table
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // parser.parseStatement();
    catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /*
   * return column name and column type, comma separated object
   */
  public String parseColumnTypes(String command) {
    SQLParser parser;
    String columnsNameType = "";
    // get selected columns, just simply list all columns here and add to scan object
    HashMap<String, String> colTypes = new HashMap<String, String>();
    // replace : to _
    command = command.replace(":", "_");

    try {
      // make sure input query for SQLParser doesn't contain semi colon
      parser = new SQLParser(new StringReader(command));
      SelectStatement stm = parser.parseQuery();
      List<AliasedNode> nodes = stm.getSelect();
      Iterator<AliasedNode> it = nodes.iterator();
      while (it.hasNext()) {
        AliasedNode node = it.next();
        String column = node.getNode().getAlias().toLowerCase();
        System.out.println(">>>>>>>>>>>>>>> column  = " + column);
        if (column.contains("_")) {
          colTypes.put(column, "string");
        } else {
          colTypes.put(column, "string");
        }
      }

      // put column type from where condition, this will override pre-set selected column's types
      ParseNode where = stm.getWhere();
      if (where != null && where.getChildren() != null && where.getChildren().size() > 0) {
        List<ParseNode> children = where.getChildren();
        Iterator<ParseNode> it2 = children.iterator();
        while (it2.hasNext()) {
          ParseNode child = it2.next();
          colTypes = parseFilterForColumnType(child, colTypes);
        }
      }

      // from colTypes, generate columnsNameType
      // TODO
      Iterator<String> cit = colTypes.keySet().iterator();
      while (cit.hasNext()) {
        String ctemp = cit.next();
        String ctype = colTypes.get(ctemp);
        columnsNameType += ctemp + " " + ctype + ",";
      }
      // remove last comma
      if (!columnsNameType.equals("") && columnsNameType.contains(",")) {
        columnsNameType = columnsNameType.substring(0, columnsNameType.length() - 1);
      }
      System.out.println(">>>>>>>>>>>>>. columnsNameType=" + columnsNameType);
      return columnsNameType;
      // get table
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // parser.parseStatement();
    catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  // TODO use must pass one first, if we dont' have OR then change to must pass all
  public FilterList parseFilter(ParseNode filterNode, FilterList list) {
    CompareOp op = null;// getOperator(filter);

    if (filterNode instanceof GreaterThanParseNode) {
      op = CompareOp.GREATER;
    } else if (filterNode instanceof GreaterThanOrEqualParseNode) {
      op = CompareOp.GREATER_OR_EQUAL;
    } else if (filterNode instanceof NotEqualParseNode) {
      op = CompareOp.NOT_EQUAL;
    } else if (filterNode instanceof EqualParseNode) {
      op = CompareOp.EQUAL;
    }

    String whereColumnName = "";
    String wherevalue = "";
    List<ParseNode> children = filterNode.getChildren();
    Iterator<ParseNode> it = children.iterator();
    while (it.hasNext()) {
      ParseNode node = it.next();
      if (node instanceof ColumnParseNode) {
        whereColumnName = ((ColumnParseNode) node).getName();
        System.out.println("where column name = " + whereColumnName);
      } else if (node instanceof LiteralParseNode) {
        String atype = ((LiteralParseNode) node).getType().name();
        System.out.println("atype = " + atype);
        wherevalue = ((LiteralParseNode) node).getValue().toString();
      }
      // current node is still compound node
      else {
        list = parseFilter(node, list);
      }
    }

    // parse
    if (whereColumnName != null && !whereColumnName.equals("") && wherevalue != null && !wherevalue.equals("")) {
      String wherecf = whereColumnName.split("_")[0];
      String wherecq = whereColumnName.split("_")[1];

      System.out.println(">>>>>>>>>> column family : " + wherecf + "\t column qualifier:" + wherecq + "\t wherevalue: "
          + wherevalue + "\t operator = " + op.toString());

      SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(wherecf), Bytes.toBytes(wherecq), op,
          Bytes.toBytes(wherevalue));
      list.addFilter(filter1);
    }
    return list;
  }

  // TODO use must pass one first, if we dont' have OR then change to must pass all
  public HashMap<String, String> parseFilterForColumnType(ParseNode filterNode, HashMap<String, String> colTypes) {
    String whereColumnName = "";
    String whereColumnType = "";
    List<ParseNode> children = filterNode.getChildren();
    Iterator<ParseNode> it = children.iterator();
    while (it.hasNext()) {
      ParseNode node = it.next();
      if (node instanceof ColumnParseNode) {
        whereColumnName = ((ColumnParseNode) node).getName();
        System.out.println("where column name = " + whereColumnName);
      } else if (node instanceof LiteralParseNode) {
        // TODO support other column type
        PDataType type = ((LiteralParseNode) node).getType();
        if (type == PDataType.INTEGER || type == PDataType.DOUBLE || type == PDataType.FLOAT || type == PDataType.LONG
            || type == PDataType.DECIMAL || type == PDataType.SMALLINT || type == PDataType.TINYINT) {
          whereColumnType = "double";
        }
      }
      // current node is still compound node
      else {
        colTypes = parseFilterForColumnType(filterNode, colTypes);
      }
    }
    // parse
    if (whereColumnName != null && !whereColumnName.equals("") && whereColumnType != null
        && !whereColumnType.equals("")) {
      String wherecf = whereColumnName.split("_")[0];
      String wherecq = whereColumnName.split("_")[1];
      System.out.println(">>>>>>>>>> column family : " + wherecf + "\t column qualifier:" + wherecq
          + "\t whereColumnType: " + whereColumnType);
      if (colTypes.containsKey(whereColumnName.toLowerCase())) colTypes.remove(whereColumnName.toLowerCase());
      colTypes.put(whereColumnName.toLowerCase(), whereColumnType);

    }
    return colTypes;
  }

  private String parseHbaseTable(String command) {
    String tableName = "";
    SQLParser parser;
    command = command.replace(":", "_");
    try {
      parser = new SQLParser(new StringReader(command));
      SelectStatement stm = parser.parseQuery();
      Scan scan = new Scan();
      // get table name, assumption select from one table
      // TODO assumption one table
      List<TableNode> lst = stm.getFrom();
      NamedTableNode temp = (NamedTableNode) lst.get(0);
      tableName = temp.getName().getTableName().trim();

    } catch (Exception ex) {

    }
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
