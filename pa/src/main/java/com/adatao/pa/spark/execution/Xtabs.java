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


import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.AggregationHandler.AggregationResult;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.common.base.Joiner;

// For prototype/templating purpose
// This executor returns the FULL result of a query as List<String>
@SuppressWarnings("serial")
public class Xtabs extends CExecutor {
  String dataContainerID;
  String gcols;
  String scols;
  Integer maxLevels = 1000;

  public static Logger LOG = LoggerFactory.getLogger(Xtabs.class);


  static public class Sql2ListStringResult extends SuccessResult {
    List<String> results;


    public Sql2ListStringResult setResults(List<String> results) {
      this.results = results;
      return this;
    }

    public List<String> getResults() {
      return results;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    DDFManager dm = sparkThread.getDDFManager();
    DDF ddf = (DDF) dm.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
    if (ddf == null) {
      LOG.info("Cannot find the DDF " + dataContainerID);
    } else {
      LOG.info("Found the DDF " + dataContainerID);
    }

    try {
      AggregationResult agg = null;
      if (!scols.equalsIgnoreCase("count")) {
        agg = ddf.xtabs(String.format("%s, SUM(%s)", gcols, scols));
      } else {
        agg = ddf.xtabs(String.format("%s, COUNT(*)", gcols));
      }
      List<String> res = new ArrayList<String>();
      for (String k : agg.keySet()) {
        res.add(String.format("%s,%s", k, Joiner.on(",").join(agg.get(k))));
      }
      return new Sql2ListStringResult().setResults(res);
    } catch (DDFException e) {
      throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, e.getMessage(), null);
    }

  }

  public Xtabs setSqlCmd(String dataContainerID, String gcols, String scols) {
    this.dataContainerID = dataContainerID;
    this.gcols = gcols;
    this.scols = scols;
    return this;
  }
}
