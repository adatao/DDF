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


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class VectorCovariance extends CExecutor {
  private String dataContainerID;
  private String xColumn;
  private String yColumn;
  
  public VectorCovariance setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
  
  public VectorCovariance setXColumn(String xColumn) {
    this.xColumn = xColumn;
    return this;
  }
  
  public VectorCovariance setYColumn(String yColumn) {
    this.yColumn = yColumn;
    return this;
  }

  public static Logger LOG = LoggerFactory.getLogger(VectorCovariance.class);
  static public class VectorCovarianceResult extends SuccessResult {
    public double covariance;
    public VectorCovarianceResult(double covariance) {
      this.covariance = covariance;
    }
    public double getCovariance() {
      return covariance;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) {
    
    DDFManager ddfManager = sparkThread.getDDFManager();
    
    DDF ddf = ddfManager.getDDF(dataContainerID);
    
    double result;
    try {
      result = ddf.getVectorCovariance(xColumn, yColumn);
      
      return new VectorCovarianceResult(result);
    } catch (DDFException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
}


