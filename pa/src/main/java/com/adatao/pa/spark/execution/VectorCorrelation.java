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
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.VectorCorrelation.VectorCorrelationResult;
import com.adatao.pa.spark.execution.VectorVariance.VectorVarianceResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class VectorCorrelation extends CExecutor {
  private String xDataContainerID;
  private String yDataContainerID;
  
  public VectorCorrelation setXDataContainerID(String dataContainerID) {
    this.xDataContainerID = dataContainerID;
    return this;
  }
  
  public VectorCorrelation setYDataContainerID(String dataContainerID) {
    this.yDataContainerID = dataContainerID;
    return this;
  }

  public static Logger LOG = LoggerFactory.getLogger(VectorCorrelation.class);
  static public class VectorCorrelationResult extends SuccessResult {
    Double correlation;
    public VectorCorrelationResult(double correlation) {
      this.correlation = correlation;
    }
    public Double getCorrelation() {
      return correlation;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    
    DDFManager ddfManager = sparkThread.getDDFManager();
    String ddfId = Utils.dcID2DDFID(xDataContainerID);
    String otherddfId = Utils.dcID2DDFID(yDataContainerID);
    
    DDF ddf = ddfManager.getDDF(ddfId);
    DDF otherddf = ddfManager.getDDF(otherddfId);
    
    String xColumn = ddf.getSchema().getColumn(0).getName();
    String yColumn = otherddf.getSchema().getColumn(0).getName();
    
    Double result;
    try {
      result = ddf.getVectorCor(xColumn, yColumn);
      return new VectorCorrelationResult(result);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}


