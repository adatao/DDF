/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark;


import java.util.List;
import com.adatao.pa.AdataoException;
import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.exception.DDFException;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.types.SuccessResult;



public class Utils {

  public static void printDoubleArray(String title, String fmt, double[] xs) {
    System.out.print(title + " ");
    for (double x : xs) {
      System.out.format(fmt, x);
    }
    System.out.println();
  }

  public static void assertNull(Object o, AdataoException e) throws AdataoException {
    if (o == null) throw e;
  }

  public static void assertNullorEmpty(List o, AdataoException e) throws AdataoException {
    if (o == null || o.size() == 0) throw e;
  }
  
  public static MetaInfo[] generateMetaInfo(Schema schema) throws DDFException {
    List<Column> columns = schema.getColumns();
    MetaInfo[] metaInfo = new MetaInfo[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
      
      if (columns.get(i).getColumnClass() == ColumnClass.FACTOR) {
        metaInfo[i].setFactor(columns.get(i).getOptionalFactor().getLevelMap());
      }
    }
    return metaInfo;
  }

  public static String getDataContainerID(DDF ddf) {
    return ddf.getName();
  }

  public static String getDDFNameFromDataContainerID(String dataContainerID) {
    return dataContainerID;
  }
  
  public static String reindent(String value, int totalIndent) {
    String delimiter = " ";
    int left = totalIndent - value.length();
    StringBuilder sb = new StringBuilder();
    sb.append(value);
    if(left > 0) {
      for(int i=0; i< left; i++) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }
  
  public static  String reindent(Double v, int totalIndent) {
    String delimiter = " ";
    String value = String.valueOf(v);
    int left = totalIndent - value.length();
    StringBuilder sb = new StringBuilder();
    sb.append(value);
    if(left > 0) {
      for(int i=0; i< left; i++) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }
  
  public static  String reindent(long v, int totalIndent) {
    String delimiter = " ";
    String value = String.valueOf(v);
    int left = totalIndent - value.length();
    StringBuilder sb = new StringBuilder();
    sb.append(value);
    if(left > 0) {
      for(int i=0; i< left; i++) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }
  static public class DataFrameResult extends SuccessResult {
    public String dataContainerID;
    public MetaInfo[] metaInfo;

    public DataFrameResult(DDF ddf) throws DDFException {
      this.dataContainerID = ddf.getName();
      this.metaInfo = generateMetaInfo(ddf.getSchema());
    }
    
    public DataFrameResult(String dataContainerID, MetaInfo[] metaInfo) throws DDFException {
      this.dataContainerID = dataContainerID;
      this.metaInfo = metaInfo;
    }
    
    public String getDataContainerID() {
      return dataContainerID;
    }


    public MetaInfo[] getMetaInfo() {
      return metaInfo;
    }
  }
  
  static public class MutableDataFrameResult extends SuccessResult {
    public String dataContainerID;
    public MetaInfo[] metaInfo;
    public boolean isMutable;

    public MutableDataFrameResult(DDF ddf) throws DDFException {
      this.dataContainerID = ddf.getName();
      this.metaInfo = generateMetaInfo(ddf.getSchema());
      this.isMutable =  ddf.isMutable();
    }
    
    public MutableDataFrameResult(String dataContainerID, MetaInfo[] metaInfo, boolean isMutable) throws DDFException {
      this.dataContainerID = dataContainerID;
      this.metaInfo = metaInfo;
      this.isMutable = isMutable;
    }
    
    public String getDataContainerID() {
      return dataContainerID;
    }


    public MetaInfo[] getMetaInfo() {
      return metaInfo;
    }
    
    public boolean isMutable() {
      return isMutable;
    }
  }

}
