/**
 * 
 */
package com.adatao.spark.ddf.etl;

import java.util.List;

import org.apache.spark.rdd.RDD;

import scala.collection.Seq;
import shark.SharkContext;
import shark.api.Row;
import shark.api.TableRDD;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.ASqlHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.spark.ddf.content.SchemaHandler;
import com.google.common.base.Strings;

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

    // TODO: handle other dataSources and dataFormats

    String tableName = this.getDDF().getSchemaHandler().newTableName();
    if (tableName != null) {
      tableName = tableName.replace("-", "_");
    }
    if (dataSource == null) {
      String sqlCmd;

      sqlCmd = String.format(
                            "CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_AND_DISK\") AS %s",
                                                    tableName, command);
      tableRdd = this.getSharkContext().sql2rdd(sqlCmd);

    } else {
      // TODO
    }

    RDD<Row> rdd = (RDD<Row>) tableRdd;

    if (schema == null) schema = SchemaHandler.getSchemaFrom(tableRdd.schema());
    /*
    String tableName = (schema != null ? schema.getTableName() : null);
    
    if (Strings.isNullOrEmpty(tableName)) tableName = (rdd != null ? rdd.name() : null);
    if (Strings.isNullOrEmpty(tableName)) tableName = this.getDDF().getSchemaHandler().newTableName();
    i*/
    if (tableName != null) {
      schema.setTableName(tableName);
    }
    
    return new SparkDDF(this.getManager(), rdd, Row.class, null, tableName, schema);
  }

  private <T> List<T> toList(Seq<T> sequence) {
    return scala.collection.JavaConversions.seqAsJavaList(sequence);
  }

  public static final int MAX_COMMAND_RESULT_ROWS = 1000;

  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.sql2txt(command, null);
  }

  @Override
  public List<String> sql2txt(String command, String dataSource) throws DDFException {
    // TODO: handle other dataSources
    try {
      return this.toList(getSharkContext().sql(command, MAX_COMMAND_RESULT_ROWS));

    } catch (Exception e) {
      throw new DDFException(e);
    }
  }
}
