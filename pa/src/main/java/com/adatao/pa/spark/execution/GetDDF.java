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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
import io.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils.MutableDataFrameResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class GetDDF extends CExecutor {
  String ddfName;

  public static Logger LOG = LoggerFactory.getLogger(GetDDF.class);


  public GetDDF(String ddfName) {
    this.ddfName = ddfName;
  }  

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (ddfName == null) {
      return new FailResult().setMessage("ddfName string is empty");
    }
    try {
      if(ddfName.startsWith("ddf://")) {
        int lastIdx = ddfName.lastIndexOf("/");
        ddfName = ddfName.substring(lastIdx + 1);
      }

      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.getDDF(ddfName);
      if (ddf != null) {
        LOG.info("succesful getting ddf from name = " + ddfName);
      } else {
        LOG.info("Can not get ddf from name = " + ddfName);
      }
      return new MutableDataFrameResult(ddf);

    } catch (Exception e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
      if (e instanceof shark.api.QueryExecutionException) {
        throw new AdataoException(AdataoExceptionCode.ERR_GETDDF_FAILED, e.getMessage(), null);
      } else {
        LOG.error("Cannot create a ddf from the sql command", e);
        return null;
      }
    }
  }
}
