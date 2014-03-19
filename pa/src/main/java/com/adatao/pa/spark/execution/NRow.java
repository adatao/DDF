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
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class NRow extends CExecutor {
  private String dataContainerID;

  // private String ddfName;

  public static Logger LOG = LoggerFactory.getLogger(NRow.class);


  static public class NRowResult extends SuccessResult {
    public long nrow;


    public NRowResult(long nrow) {
      this.nrow = nrow;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException, DDFException {
    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
    return new NRowResult(ddf.getNumRows());
  }

  public NRow setDataContainerID(String ddfName) {
    // this.ddfName = ddfName;
    return this;
  }

}
