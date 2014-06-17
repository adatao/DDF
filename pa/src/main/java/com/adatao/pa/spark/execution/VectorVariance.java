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


import com.adatao.ML.Utils;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
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
    String ddfId = Utils.dcID2DDFID(dataContainerID);
    DDF ddf = ddfManager.getDDF(ddfId);
    Double[] result;
    try {
      result = ddf.getVectorVariance(columnName);
      return new VectorVarianceResult().setDataContainerID(dataContainerID).setVariance(result[0]).setStddev(result[1]);
    } catch (DDFException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
}
