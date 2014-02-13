package com.adatao.ddf.spark;

import org.apache.spark.api.java.JavaSparkContext;

import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFContext;
import com.adatao.ddf.exception.DDFException;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 * 
 */
public class SparkDDFContext implements DDFContext {
    private JavaSparkContext sc;
    
    public SparkDDFContext(JavaSparkContext sc) {
        this.sc = sc;
    }
    
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
    @Override
    public DDF createSchema(String schemaString) throws DDFException {
        // TODO: @bhan or @huan
        // please add your implementation of creating a DDF from a schema here

        return new SparkDDFImpl();
    }

    /**
     * Create a DDF from "select" queries.
     * 
     * An example query looks like.
     * select * from airline
     */
    @Override
    public DDF fromSql(String sqlCommand) throws DDFException {
        // TODO: @bhan or @huan
        // please add your implementation of creating a DDF from an SQL query here.

        return new SparkDDFImpl();
    }
    
    /**
     * read a CSV file from HDFS file system or local file system and return
     * a DDF which represent it.
     * 
     * This method will read the CSV and try to guess the table schema out of it.
     * 
     * @return the DDF represents the CSV data.
     */
    @Override
    public DDF readCSV(String csvFile) throws DDFException {
        return new SparkDDFImpl();
    }

    public void shutdown() {
        if (sc != null) {
            sc.stop();
        }
    }

}
