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
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class GetDDF extends CExecutor {
  String ddfName;
  Boolean cache = true;

  public static Logger LOG = LoggerFactory.getLogger(Sql2DataFrame.class);


  public GetDDF(String ddfName) {
    this.ddfName = ddfName;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (ddfName == null) {
      return new FailResult().setMessage("ddfName string is empty");
    }
    try {
      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.getDDF(ddfName);
      if(ddf != null) {
    	  System.out.println(">>>>>>>>> succesful getting ddf from name = " + ddfName);
      }
      else
    	  System.out.println(">>>>>>>>> can not get ddf from name = " + ddfName);

      System.out.println(">>>>>>>>> getting ddf from name = " + ddfName);
      //set Name
      ddf.setName(ddfName);
      
      
      return new Sql2DataFrameResult(ddf);

    } catch (Exception e) {
      // I cannot catch shark.api.QueryExecutionException directly
      // most probably because of the problem explained in this
      // http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
      if (e instanceof shark.api.QueryExecutionException) {
        throw new AdataoException(AdataoExceptionCode.ERR_LOAD_TABLE_FAILED, e.getMessage(), null);
      } else {
        LOG.error("Cannot create a ddf from the sql command", e);
        return null;
      }
    }
  }
}
