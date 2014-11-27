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
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

// For prototype/templating purpose
// This executor returns the FULL result of a query as List<String>
@SuppressWarnings("serial")
public class Sql2ListString extends CExecutor {
  String sqlCmd;

  public static Logger LOG = LoggerFactory.getLogger(Sql2ListString.class);


  public static class Sql2ListStringResult extends SuccessResult {
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
    if (sqlCmd == null) {
      return new FailResult().setMessage("Sql command string is empty");
    }

    DDFManager dm = sparkThread.getDDFManager();
    try {
      List<String> results = dm.sql2txt(sqlCmd);
      if (sqlCmd.matches("^\\s*show\\s+tables\\s*$")) {
        List<String> toRemove = new ArrayList<String>(results.size());
        // filter out ^bigrdf.+
        for (String s : results) {
          if (s.matches("^bigrdf.+$")) {
            toRemove.add(s);
          }
        }
        results.removeAll(toRemove);
      }
      return new Sql2ListStringResult().setResults(results);
    } catch (DDFException e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
      throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, e.getMessage(), null);
    }
  }


  public Sql2ListString setSqlCmd(String sqlCmd) {
    this.sqlCmd = sqlCmd;
    return this;
  }
}
