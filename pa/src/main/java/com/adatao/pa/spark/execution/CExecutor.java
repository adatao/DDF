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

/**
 * 
 */
package com.adatao.pa.spark.execution;


import java.io.Serializable;
import java.lang.reflect.Field;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.google.gson.Gson;

/**
 * Base implementation for all Executors, with helper methods for extracting parameters, etc.
 * 
 * @author ctn
 * 
 *         This class is going to be deprecated and replaced by AExecutor
 */
@SuppressWarnings("serial")
@Deprecated
public abstract class CExecutor implements IExecutor, Serializable {

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.pa.spark.types.IExecutor#run(com.adatao.pa.spark.SparkThread)
   */
  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Default implementation: prints out class name and all public fields
   */
  @Override
  public String toString() {
    StringBuilder output = new StringBuilder(this.getClass().getSimpleName());
    output.append("[");

    Gson gson = new Gson();

    Field[] fields = this.getClass().getFields();

    for (Field field : fields) {
      output.append(field.getName());

      output.append("=");

      try {
        output.append(gson.toJson(field.get(this)));
      } catch (Exception e) {
        output.append(e.getMessage());
      }

      output.append(",");
    }

    output.append("]");

    return output.toString();
  }
}
