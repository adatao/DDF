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


import com.adatao.pa.spark.types.SuccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils.DataFrameResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class SetDDFName extends CExecutor {
  String dataContainerId;
  String ddfName;

  public static Logger LOG = LoggerFactory.getLogger(Sql2DataFrame.class);


  public SetDDFName(String dataContainerId, String ddfName) {
    this.dataContainerId = dataContainerId;
    this.ddfName = ddfName;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (ddfName == null) {
      return new FailResult().setMessage("ddfName string is empty");
    }
    try {
      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.getDDF(dataContainerId);


      if (ddf != null) {
        LOG.info(" succesful setting ddf to alias name = " + ddfName);
        ddf.setName(ddfName);
      } else LOG.error("Can not set ddf to alias name = " + ddfName);

      return new SetDDFNameResult(ddf.getUri());

    } catch (Exception e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
      return null;
    }
  }

  public static class SetDDFNameResult extends SuccessResult {
    public String uri;

    public SetDDFNameResult(String uri) {
      this.uri = uri;
    }
  }
}
