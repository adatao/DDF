package com.adatao.ddf.spark;

import org.apache.spark.rdd.RDD;

import shark.api.JavaTableRDD;
import shark.memstore2.TablePartition;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;

/**
 * SparkDDF extends DDF in order to provide some utility methods that are specific to Spark
 * framework.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
public class SparkDDF extends DDF {

  /**
   * We could add internal methods that only specific for Spark DDF here. Such method should only be
   * used within the spark module. They should not be used outside of this module.
   */

  private RDD<TablePartition> mDataTable;
  private Schema mSchema;
  public SparkDDF (RDD<TablePartition> data) {
    this.mDataTable = data;
  }
  public SparkDDF(RDD<TablePartition> data, Schema theSchema) {
    this.mDataTable = data;
    this.mSchema = theSchema;
  }
  public RDD<TablePartition> getRDD() {
    return mDataTable;
  }

}
