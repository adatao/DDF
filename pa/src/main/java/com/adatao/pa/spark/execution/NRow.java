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


import io.ddf.exception.DDFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
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
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(dataContainerID);
    if (ddf == null) {
      LOG.error("Cannot find the DDF " + dataContainerID);
    } else {
      LOG.info("Found the DDF " + dataContainerID);
    }
    try {
      return new NRowResult(ddf.getNumRows());
    } catch(Exception e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL,"Error getting NumRows", null);
    }
  }

  public NRow setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

}
