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
import com.adatao.ddf.exception.DDFException;
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
    // public String dataContainerID;
    // public MetaInfo[] metaInfo;
    // public Sql2DataFrameResult(String dataContainerID, SharkDataFrame df) {
    // this.dataContainerID = dataContainerID;
    // this.metaInfo = df.getMetaInfo();
    // }
    public String dataContainerID;
    public MetaInfo[] metaInfo;


    public Sql2DataFrameResult(DDF ddf) {
      this.dataContainerID = ddf.getName().substring(15);
      this.metaInfo = generateMetaInfo(ddf.getSchema());
    }

    private MetaInfo[] generateMetaInfo(Schema schema) {
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

    // SharkDataFrame df = new SharkDataFrame();

    try {
      // JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
      // df.loadTableFromQuery(sc, sqlCmd, cache);

      // DataManager dm = sparkThread.getDataManager();
      // String dataContainerID = dm.add(df);
      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.sql2ddf(sqlCmd);
      String ddfName = ddfManager.addDDF(ddf);

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
