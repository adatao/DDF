package com.adatao.pa.spark.execution;
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


import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.Sql2ListString.Sql2ListStringResult;
import com.adatao.pa.spark.types.ExecutorResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class ListDDF extends CExecutor {
  public static Logger LOG = LoggerFactory.getLogger(ListDDF.class);
  String test;
  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {
      DDFManager ddfManager = sparkThread.getDDFManager();
      List<String> results = ddfManager.listDDFs();
      if (results != null) {
        LOG.info("succesful getting ddf from name");
      } else {
        LOG.info("Can not get ddf from name");
      }
      return new Sql2ListStringResult().setResults(results);

    } catch (Exception e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
        LOG.error("Cannot get list of DDFs", e);
        return null;
    }
  }
}
