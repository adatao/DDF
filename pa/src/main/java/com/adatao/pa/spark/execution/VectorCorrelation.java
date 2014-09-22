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


import java.util.List;

import com.adatao.pa.AdataoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.spark.ddf.analytics.Utils;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class VectorCorrelation extends CExecutor {
  private String dataContainerID;
  private String xColumn;
  private String yColumn;
  
  public VectorCorrelation setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
  
  public VectorCorrelation setXColumn(String xColumn) {
    this.xColumn = xColumn;
    return this;
  }
  
  public VectorCorrelation setYColumn(String yColumn) {
    this.yColumn = yColumn;
    return this;
  }

  public static Logger LOG = LoggerFactory.getLogger(VectorCorrelation.class);
  static public class VectorCorrelationResult extends SuccessResult {
    public double correlation;
    public VectorCorrelationResult(double correlation) {
      this.correlation = correlation;
    }
    public double getCorrelation() {
      return correlation;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    
    DDFManager ddfManager = sparkThread.getDDFManager();
//    String ddfId = Utils.dcID2DDFID(dataContainerID);
//    String otherddfId = Utils.dcID2DDFID(yDataContainerID);
    
    DDF ddf = ddfManager.getDDF(dataContainerID);
    
//    String xColumn = ddf.getSchema().getColumn(0).getName();
//    String yColumn = otherddf.getSchema().getColumn(0).getName();
    
    double result;
    try {
      result = ddf.getVectorCor(xColumn, yColumn);
      return new VectorCorrelationResult(result);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}


