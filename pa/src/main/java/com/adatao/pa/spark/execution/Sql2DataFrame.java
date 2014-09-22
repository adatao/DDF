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
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class Sql2DataFrame extends CExecutor {
  String sqlCmd;
  Boolean cache = true;

  public static Logger LOG = LoggerFactory.getLogger(Sql2DataFrame.class);


  public Sql2DataFrame(String sqlCmd, Boolean cache) {
    this.sqlCmd = sqlCmd;
    this.cache = cache;
  }


  static public class Sql2DataFrameResult extends SuccessResult {
    
    public String dataContainerID;
    public MetaInfo[] metaInfo;


    public Sql2DataFrameResult(DDF ddf) {
      this.dataContainerID = ddf.getName();
      this.metaInfo = generateMetaInfo(ddf.getSchema());
    }

    public static MetaInfo[] generateMetaInfo(Schema schema) {
      List<Column> columns = schema.getColumns();
      MetaInfo[] metaInfo = new MetaInfo[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
      }
      return metaInfo;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (sqlCmd == null) {
      return new FailResult().setMessage("Sql command string is empty");
    }

    try {

      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.sql2ddf(sqlCmd);
      String ddfName = ddfManager.addDDF(ddf);
      LOG.info("DDF Name: " + ddfName);

      return new Sql2DataFrameResult(ddf);

    } catch (Exception e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }
}
