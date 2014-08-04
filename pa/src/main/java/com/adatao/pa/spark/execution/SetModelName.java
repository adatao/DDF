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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.GetURI.StringResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class SetModelName extends CExecutor {
  String modelId;
  String modelName;

  public static Logger LOG = LoggerFactory.getLogger(Sql2DataFrame.class);


  public SetModelName(String modelId, String modelName) {
    this.modelId = modelId;
    this.modelName = modelName;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (modelName == null) {
      return new FailResult().setMessage("modelName string is empty");
    }
    
      DDFManager ddfManager = sparkThread.getDDFManager();
      ddfManager.setModelName(modelId, modelName);
      return new StringResult("object://adatao.com/" +modelName);

  }
}

