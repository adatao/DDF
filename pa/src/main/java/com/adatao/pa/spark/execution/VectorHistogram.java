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
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import java.util.List;
import com.google.common.collect.Lists;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;

@SuppressWarnings("serial")
public class VectorHistogram extends CExecutor {
  private String dataContainerID;
  private String columnName;
  private int numBins;


  public VectorHistogram setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

  public VectorHistogram setColumnName(String columnName) {
    this.columnName = columnName;
    return this;
  }

  public VectorHistogram setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }


  public static Logger LOG = LoggerFactory.getLogger(VectorHistogram.class);


  static public class VectorHistogramResult extends SuccessResult {
    String dataContainerID;
    List<HistogramBin> histogramBins;


    public List<HistogramBin> getHistogramBins() {
      return histogramBins;
    }

    public VectorHistogramResult setHistogramBins(List<HistogramBin> histogramBins) {
      this.histogramBins = histogramBins;
      return this;
    }

    public String getDataContainerID() {
      return dataContainerID;
    }

    public VectorHistogramResult setDataContainerID(String dataContainerID) {
      this.dataContainerID = dataContainerID;
      return this;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    DDFManager ddfManager = sparkThread.getDDFManager();

    DDF ddf = ddfManager.getDDF(dataContainerID);

    List<HistogramBin> bins = Lists.newArrayList();
    try {
      bins = ddf.getVectorHistogram(columnName, numBins);

      return new VectorHistogramResult().setDataContainerID(dataContainerID).setHistogramBins(bins);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}
