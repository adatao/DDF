package com.adatao.spark.ddf;

import org.apache.spark.rdd.RDD;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

/**
 * DDF extends DDF in order to provide some utility methods that are specific to Spark
 * framework.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
public class SparkDDF extends DDF {

  public <T> SparkDDF(RDD<T> rdd, Class<T> elementType) throws DDFException {
    this.initialize(rdd, elementType, null);
  }

  public <T> SparkDDF(RDD<T> rdd, Class<T> elementType, Schema schema) throws DDFException {
    this.initialize(rdd, elementType, schema);
  }


  private <T> void initialize(RDD<T> rdd, Class<T> elementType, Schema schema) throws DDFException {
    if (rdd == null) throw new DDFException("Non-null RDD is required to instantiate a new DDF");
    if (rdd.sparkContext() == null) throw new DDFException("SparkContext is required to instantiate a new DDF");
    this.setManager(new SparkDDFManager(rdd.sparkContext()));
    this.getManager().setDDF(this);
    this.getRepresentationHandler().set(rdd, elementType);
    this.getSchemaHandler().setSchema(schema);
  }

  @SuppressWarnings("unchecked")
  public <T> RDD<T> getRDD(Class<T> elementType) throws DDFException {
    Object obj = this.getRepresentationHandler().get(elementType);
    if (obj instanceof RDD<?>) return (RDD<T>) obj;
    else throw new DDFException("Unable to get RDD with element type " + elementType);
  }
}
