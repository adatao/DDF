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

package com.adatao.pa.spark.types;

import org.junit.Test;

import com.adatao.ML.LinearRegressionModel;
import com.adatao.ddf.types.Vector;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.spark.types.SuccessfulResult;

/**
 * To test our various serdes support, as a Java client
 * 
 * @author ctn
 * 
 */
public class TestSerialization {

  @Test
  public void testExecutionResult() {
    LinearRegressionModel lrm = new LinearRegressionModel(new Vector(10), new Vector(5), 10);
    ExecutionResult<LinearRegressionModel> xr = new SuccessfulResult<LinearRegressionModel>(lrm);
    System.out.println(xr.toJson());
  }
}
