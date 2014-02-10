package com.adatao.ddf.spark

import org.apache.spark.SparkContext
import com.adatao.ddf.DDF

class SparkDDFFactory(val sc: SparkContext) extends com.adatao.ddf.DDFFactory {
  /**
   * Create an empty DDF with a schema definition.
   * 
   * An example query looks like:
   * create table airline (v1 int, v2 double, v3 double, v4 double,
   * v5 double, v6 double, v7 double, v8 double, v9 string, v10 double, 
   * v11 string, v12 double, v13 double, v14 double, v15 double, v16 double, 
   * v17 string, v18 string, v19 double, v20 double, v21 double, v22 double, 
   * v23 double, v24 double, v25 double, v26 double, v27 double, v28 double, v29 double)
   * row format delimited fields terminated by ','"
   */
  def createSchema(schemaString: java.lang.String): DDF = {
    // TODO: @bhan or @huan
    // please add your implementation of creating a DDF from a schema here
    
    return new SparkDDFImpl
  }

  /**
   * Create a DDF from "select" queries.
   * 
   * An example query looks like.
   * select * from airline
   */
  def fromSql(sqlCommand: java.lang.String): DDF = {
    return new SparkDDFImpl
  }
}