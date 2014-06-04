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


import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.types.SuccessResult;

/**
 * Class to get factor of one input column in a SharkDataFrame
 * <p>
 * The generated factor is stored at the master as part of the data frame metainfo. We only support getting factor if
 * the number of levels is no bigger than 1K, this is to make sure that we can store the levels at the master as an
 * array and not as an RDD
 * 
 * @author Michael Bach Bui <freeman@adatao.com>
 * @since 06-21-2013
 * @return Map&lt;String,Integer> map of levels (as a string) to the number that that level occurs in the column
 */
@SuppressWarnings("serial")
public class GetFactor extends CExecutor {
  private String dataContainerID;
  private Integer columnIndex;
  private String columnName;
  // private boolean ordered;
  // private String[] labels;

  public static Logger LOG = LoggerFactory.getLogger(GetFactor.class);
  static final int MAX_LEVEL_SIZE = Integer.parseInt(System.getProperty("factor.max.level.size", "1024"));


  public static class GetFactorResult extends SuccessResult {
    Map<String, Integer> factor;


    public Map<String, Integer> getFactor() {
      return factor;
    }

    public GetFactorResult setFactor(Map<String, Integer> factor) {
      this.factor = factor;
      return this;
    }
  }
}
