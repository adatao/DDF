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

package com.adatao.pa.spark.execution;


import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import com.adatao.ML.types.TJsonSerializable;
import com.adatao.ML.types.TJsonSerializable$class;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import com.adatao.ddf.util.Utils;

/**
 * @author bachbui Implement summary function for both vector and dataframe
 */
@SuppressWarnings("serial")
public class QuickSummary extends CExecutor {
  private String dataContainerID;
  DDFManager ddfManager;

  public static Logger LOG = LoggerFactory.getLogger(QuickSummary.class);


  public static class DataframeStatsResult extends SuccessResult implements TJsonSerializable, Serializable {

    String dataContainerID;
    // StatCounter
    public double[] mean;
    public double[] sum;
    public double[] stdev;
    public double[] var;
    public long[] cNA;
    public long[] count;
    public double[] min;
    public double[] max;
    String clazz;
    List<String> colNames;
    
    public String delimiter = " ";
    public int totalIndent = 14;


    public void setColnames(java.util.List<String> colnames) {
      colNames = colnames;
    }

    public String clazz() {
      return clazz;
    }

    public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
      clazz = Aclass;
    }


    public TJsonSerializable fromJson(String jsonString) {
      return TJsonSerializable$class.fromJson(this, jsonString);
    }

    public void setStats(Summary[] stats) {
      int dim = stats.length;
      mean = new double[dim];
      sum = new double[dim];
      stdev = new double[dim];
      var = new double[dim];
      cNA = new long[dim];
      count = new long[dim];
      min = new double[dim];
      max = new double[dim];

      for (int i = 0; i < stats.length; i++) {
        if (stats[i] != null) {
          mean[i] = Utils.roundUp(stats[i].mean());
          sum[i] = Utils.roundUp(stats[i].sum());
          stdev[i] = Utils.roundUp(stats[i].stdev());
          var[i] = Utils.roundUp(stats[i].variance());
          cNA[i] = stats[i].NACount();
          count[i] = stats[i].count();
          min[i] = Utils.roundUp(stats[i].min());
          max[i] = Utils.roundUp(stats[i].max());
        }
      }
    }

    public String getDataContainerID() {
      return dataContainerID;
    }

    public DataframeStatsResult setDataContainerID(String dataContainerID) {
      this.dataContainerID = dataContainerID;
      this.com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(this.getClass().getName());
      return this;
    }
    
    public String reindent(String value) {
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
    
    public String reindent(Double v) {
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
    
    public String reindent(long v) {
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

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(reindent("column"));
      sb.append(reindent("mean"));
      sb.append(reindent("sum"));
      sb.append(reindent("stdev"));
      sb.append(reindent("var"));
      sb.append(reindent("cNA"));
      sb.append(reindent("count"));
      sb.append(reindent("min"));
      sb.append(reindent("max"));
      sb.append("\n");

      for (int i = 0; i < mean.length; i++) {
        sb.append(reindent(colNames.get(i).trim()));
        sb.append(reindent(mean[i]));
        sb.append(reindent(sum[i]));
        sb.append(reindent(stdev[i]));
        sb.append(reindent(var[i]));
        sb.append(reindent(cNA[i]));
        sb.append(reindent(count[i]));
        sb.append(reindent(min[i]));
        sb.append(reindent(max[i]));
        
//        sb.append(mean[i]);
//        sb.append(sum[i]);
//        sb.append(stdev[i]);
//        sb.append(var[i]);
//        sb.append(cNA[i]);
//        sb.append(count[i]);
//        sb.append(min[i]);
//        sb.append(max[i]);
        sb.append("\n");
      }
      return sb.toString();
    }
  }



  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    // first get the ddf
    ddfManager = sparkThread.getDDFManager();
    DDF ddf = ddfManager.getDDF(dataContainerID);
    try {
      Summary[] ddfSummary = ddf.getSummary();

      DataframeStatsResult dfs = new DataframeStatsResult();
      // TODO cache summary in ddf's cahcedObjects
      dfs.setColnames(ddf.getColumnNames());
      dfs.setStats(ddfSummary);
      dfs.setDataContainerID(ddf.getName());

      return dfs;
    } catch (DDFException e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
      throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, e.getMessage(), null);

    }

  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public QuickSummary setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
