/**
 * 
 */
package com.adatao.spark.ddf.etl;

import org.apache.spark.rdd.RDD;

import shark.SharkContext;
import shark.api.Row;
import shark.api.TableRDD;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.APersistenceHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.SparkDDFManager;
import com.adatao.spark.ddf.content.SparkSchemaHandler;

/**
 * @author ctn
 * 
 */
public class SparkPersistenceHandler extends APersistenceHandler {

  public SparkPersistenceHandler(ADDFManager theContainer) {
    super(theContainer);
  }

  private SharkContext getSharkContext() {
    return ((SparkDDFManager) this.getContainer()).getSharkContext();
  }

  @Override
  public DDF load(String command) throws DDFException {
    TableRDD tableRdd = this.getSharkContext().sql2rdd(command);
    RDD<Row> rdd = (RDD<Row>) tableRdd;
    Schema schema = SparkSchemaHandler.getSchemaFrom(tableRdd.schema());

    return new SparkDDF(rdd, Row.class, schema);
  }

  @Override
  public DDF load(String command, Schema schema) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF load(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }
}
