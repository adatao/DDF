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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class TopN extends CExecutor {
  private String dataContainerID;
  int limit = 1000;
  String orderedCols = "";
  String mode = "asc";
  
  
  
  public String getOrderedCols() {
    return orderedCols;
  }

  public void setOrderedCols(String orderedCols) {
    this.orderedCols = orderedCols;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public static Logger LOG = LoggerFactory.getLogger(TopN.class);


  @Override
  public ExecutorResult run(SparkThread sparkThread) {

    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(dataContainerID);
    List<String> data;
    try {
      data = ddf.VIEWS.top(limit, orderedCols, mode);
      return new FetchRows.FetchRowsResult().setDataContainerID(dataContainerID).setData(data);
    } catch (Exception e) {
      LOG.error(String.format("Cannot fetch %s rows", limit), e);
    }

    return null;
  }

  public TopN setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

  public TopN setLimit(int limit) {
    this.limit = limit;
    return this;
  }
}
