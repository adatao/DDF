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

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.ADataCommandHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.spark.ddf.content.SchemaHandler;
import com.google.common.base.Strings;

/**
 * @author ctn
 * 
 */
public class DataCommandHandler extends ADataCommandHandler {

  public DataCommandHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
  }

  private SharkContext getSharkContext() {
    return ((SparkDDFManager) this.getManager()).getSharkContext();
  }

  // ////// IHandleDataCommands ////////

  @Override
  public DDF cmd2ddf(String command) throws DDFException {
    TableRDD tableRdd = this.getSharkContext().sql2rdd(command);
    RDD<Row> rdd = (RDD<Row>) tableRdd;
    Schema schema = SchemaHandler.getSchemaFrom(tableRdd.schema());

    if (Strings.isNullOrEmpty(schema.getTableName())) {
      schema.setTableName(this.getManager().getSchemaHandler()
          .createTablename());
    }
    return new SparkDDF(rdd, Row.class, schema);
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF cmd2ddf(String command, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource)
      throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, DataFormat dataFormat)
      throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF cmd2ddf(String command, Schema schema, String dataSource,
      DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  private <T> List<T> toList(Seq<T> sequence) {
    return scala.collection.JavaConversions.seqAsJavaList(sequence);
  }

  public static final int MAX_COMMAND_RESULT_ROWS = 1000;

  @Override
  public List<String> cmd2txt(String command) throws DDFException {
    try {
      return this.toList(getSharkContext()
          .sql(command, MAX_COMMAND_RESULT_ROWS));
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }

  @Override
  public List<String> cmd2txt(String command, String dataSource)
      throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }
}
