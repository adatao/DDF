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


import io.ddf.DDF;
import io.ddf.DDFManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils.MutableDataFrameResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;

@SuppressWarnings("serial")
public class GetDDF extends CExecutor {
  String ddfUri;

  public static Logger LOG = LoggerFactory.getLogger(GetDDF.class);


  public GetDDF(String ddfUri) {
    this.ddfUri = ddfUri;
  }  

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    if (ddfUri == null) {
      return new FailResult().setMessage("ddfUri string is empty");
    }
    try {
/*      if(ddfName.startsWith("ddf://")) {
        int lastIdx = ddfName.lastIndexOf("/");
        ddfName = ddfName.substring(lastIdx + 1);
      }*/

      DDFManager ddfManager = sparkThread.getDDFManager();
      DDF ddf = ddfManager.getDDF(ddfUri);
      if (ddf != null) {
        LOG.info("Succesfully getting ddf from given URI = " + ddfUri);
        return new MutableDataFrameResult(ddf);
      } else {
        throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Error getting DDF from given URI " + ddfUri, null);
      }
    } catch (Exception e) {
        throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Error getting DDF from given URI " + ddfUri, null);
    }
  }
}
