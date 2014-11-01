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


import com.adatao.spark.ddf.analytics.Utils;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

/**
 * @author ngonpham Get variance for both vector and dataframe
 */
@SuppressWarnings("serial")
public class VectorVariance extends CExecutor { // implements IExecutor, Serializable {
  private String dataContainerID;
  private String columnName;
  
  
  public String getColumnName() {
    return columnName;
  }

  static public class VectorVarianceResult extends SuccessResult {
    String dataContainerID;
    double variance, stddev;
    String clazz;


    public String getDataContainerID() {
      return dataContainerID;
    }

    public VectorVarianceResult setDataContainerID(String dataContainerID) {
      this.dataContainerID = dataContainerID;
      return this;
    }

    public VectorVarianceResult setVariance(double variance) {
      this.variance = variance;
      return this;
    }

    public double getVariance() {
      return variance;
    }

    public VectorVarianceResult setStddev(double stddev) {
      this.stddev = stddev;
      return this;
    }

    public double getStdDev() {
      return stddev;
    }

    public String clazz() {
      return clazz;
    }

    public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
      clazz = Aclass;
    }
    // @Override
    // public TJsonSerializable fromJson(String jsonString) {
    // return TJsonSerializable$class.fromJson(this, jsonString);
    // }
  }
  public VectorVariance setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    DDFManager ddfManager = sparkThread.getDDFManager();
    DDF ddf = ddfManager.getDDF(dataContainerID);
    Double[] result;
    try {
      if(this.getColumnName() == null) this.setColumnName(ddf.getSchema().getColumn(0).getName());
      result = ddf.getVectorVariance(columnName);
      return new VectorVarianceResult().setDataContainerID(dataContainerID).setVariance(result[0]).setStddev(result[1]);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}
