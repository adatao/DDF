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
 * @author ngonpham Get mean for both vector and dataframe
 */
@SuppressWarnings("serial")
public class VectorMean  extends CExecutor { // implements IExecutor, Serializable {

  private String dataContainerID;
  private String columnName;

  public VectorMean setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

  static public class VectorMeanResult extends SuccessResult  {
    String dataContainerID;
    double mean;
    String clazz;

    public String getDataContainerID() {
      return dataContainerID;
    }

    public VectorMeanResult setDataContainerID(String dataContainerID) {
      this.dataContainerID = dataContainerID;
      return this;
    }

    public VectorMeanResult setMean(double mean) {
      this.mean = mean;
      return this;
    }

    public double getMean() {
      return mean;
    }
    public String clazz() {
      return clazz;
    }
    public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
      clazz = Aclass;
    }
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

      DDFManager ddfManager = sparkThread.getDDFManager();
      String ddfId = Utils.dcID2DDFID(dataContainerID);
      DDF ddf = ddfManager.getDDF(ddfId);
      Double mean;
      try {
        if(this.getColumnName() == null) this.setColumnName(ddf.getSchema().getColumn(0).getName());
        mean = ddf.getVectorMean(this.getColumnName());
        return new VectorMeanResult().setDataContainerID(dataContainerID).setMean(mean);
      } catch (DDFException e) {
        // TODO Auto-generated catch block
        throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
      }
      
  }
}
