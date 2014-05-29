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


import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

//TODO: write test for this command
@SuppressWarnings("serial")
public class SetColumnNames extends CExecutor {
  private String[] columnNames;
  private String dataContainerID;


  static public class SetColumnNamesResult extends SuccessResult {

  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) {
    DataManager dm = sparkThread.getDataManager();
    DataContainer dc = dm.get(dataContainerID);
    MetaInfo[] metaInfo = dc.getMetaInfo();
    if (metaInfo.length != columnNames.length) return new FailResult().setMessage("Number of columns is "
        + metaInfo.length + ". Got " + columnNames.length + " column names");
    dc.setNames(columnNames);
    return new SetColumnNamesResult();
  }

}
