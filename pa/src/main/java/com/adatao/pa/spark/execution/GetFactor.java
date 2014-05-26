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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shark.api.JavaTableRDD;
import shark.api.Row;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;

/**
 * Class to get factor of one input column in a SharkDataFrame
 * <p>
 * The generated factor is stored at the master as part of the data frame
 * metainfo. We only support getting factor if the number of levels is no bigger
 * than 1K, this is to make sure that we can store the levels at the master as
 * an array and not as an RDD
 * 
 * @author Michael Bach Bui <freeman@adatao.com>
 * @since 06-21-2013
 * @return Map&lt;String,Integer> map of levels (as a string) to the number
 *          that that level occurs in the column
 */
@SuppressWarnings("serial")
public class GetFactor extends CExecutor {
	private String dataContainerID;
	private Integer columnIndex;
	private String columnName;
//	private boolean ordered;
//	private String[] labels;

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

//	public static class FactorMapper extends FlatMapFunction<Iterator<Row>, Map<String, Integer>> {
//		String columnName = null;
//		Integer columnIndex = null;
//
//		@Override
//		public Iterable<Map<String, Integer>> call(Iterator<Row> rowIter) throws Exception {
//			Map<String, Integer> factor = new HashMap<String, Integer>();
//			ArrayList<Map<String, Integer>> res = new ArrayList<Map<String, Integer>>();
//
//			/*
//			 * this not-good-looking code is optimized for faster execution time
//			 * by avoiding having to check the value of columnName and
//			 * columnIndex at every row
//			 */
//			if (columnName != null) {
//				factor = new HashMap<String, Integer>();
//				while (rowIter.hasNext()) {
//					Row row = rowIter.next();
//					Object keyObject = row.apply(columnName);
//
//					if (keyObject != null) {
//						String key = keyObject.toString();
//						if (factor.containsKey(key)) {
//							factor.put(key, factor.get(key) + 1);
//						} else {
//							factor.put(key, 1);
//						}
//					}
//				}				
//			} else if (columnIndex != null) {
//				factor = new HashMap<String, Integer>();
//				while (rowIter.hasNext()) {
//					Row row = rowIter.next();
//					Object keyObject = row.apply(columnIndex);
//
//					if (keyObject != null) {
//						String key = keyObject.toString();
//						if (factor.containsKey(key)) {
//							factor.put(key, factor.get(key) + 1);
//						} else {
//							factor.put(key, 1);
//						}
//					}
//				}
//			}
//
//			res.add(factor);
//			return res;
//		}
//
//		public FactorMapper setColumnName(String columnName) {
//			this.columnName = columnName;
//			return this;
//		}
//
//		public FactorMapper setColumnIndex(Integer columnIndex) {
//			this.columnIndex = columnIndex;
//			return this;
//		}
//
//	}
//
//	public static class FactorReducer extends Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>> {
//
//		@Override
//		public Map<String, Integer> call(Map<String, Integer> factor1, Map<String, Integer> factor2) throws Exception {
//
//			/*
//			 * Since we only support factor with size of the levels to be no
//			 * bigger than MAX_LEVEL_SIZE, we will stop reducer when the size of
//			 * map is bigger than MAX_LEVEL_SIZE
//			 */
//			if (factor1.size() > MAX_LEVEL_SIZE) {
//				return factor1;
//			} else if (factor2.size() > MAX_LEVEL_SIZE) {
//				return factor2;
//			}
//
//			for (String key : factor1.keySet()) {
//				if (factor2.containsKey(key)) {
//					factor2.put(key, factor1.get(key) + factor2.get(key));
//				} else {
//					factor2.put(key, factor1.get(key));
//				}
//			}
//			return factor2;GetFactorResult
//		}
//
//	}
//
//	@Override
//	public ExecutorResult run(SparkThread sparkThread) {
//		DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
//
//		// TODO: we only support SharkDataFrame for now
//		@SuppressWarnings("deprecation")
////		JavaTableRDD table = ((SharkDataFrame) dc).getTableRDD();
//		JavaTableRDD table = dc.getTableRDD();
//
//		/*
//		 * Do mapPartition here to avoid mapping each row element to an String
//		 * which will consume a lot of memory. mapPartition actually acts like a
//		 * reducer
//		 */
//
//		Map<String, Integer> factor = new HashMap<String, Integer>();
//		MetaInfo columnMetaInfo;
//		FactorMapper factorMapper;
//		if (columnName != null) {
//			columnMetaInfo = dc.getColumnMetaInfoByName(columnName);
//			factorMapper = new FactorMapper().setColumnName(columnName);
//		} else if (columnIndex != null) {
//			columnMetaInfo = dc.getColumnMetaInfoByIndex(columnIndex);
//			factorMapper = new FactorMapper().setColumnIndex(columnIndex);
//		} else {
//			return new FailResult().setMessage("Column name and index is undefined");
//		}
//		
//		/*
//		 * Since we only support factor with size of the levels to be no bigger
//		 * than MAX_LEVEL_SIZE, we will discard a factor when its size is bigger
//		 * than MAX_LEVEL_SIZE. Note that when level size is bigger than
//		 * MAX_LEVEL_SIZE the factor calculation is no longer accurate (see
//		 * FactorReducer)
//		 */
//		factor = table.mapPartitions(factorMapper).reduce(new FactorReducer());
//		if (factor.size() <= MAX_LEVEL_SIZE) {
//			columnMetaInfo.setFactor(factor);
//			return new GetFactorResult().setFactor(factor);
//		} else {
//			return new FailResult().setMessage("Level size exceeds limit " + MAX_LEVEL_SIZE);
//		}				
//	}
//
//	public GetFactor setDataContainerID(String dataContainerID) {
//		this.dataContainerID = dataContainerID;
//		return this;
//	}
//
//	public GetFactor setColumnIndex(Integer columnIndex) {
//		this.columnIndex = columnIndex;
//		return this;
//	}
//
//	public GetFactor setColumnName(String columnName) {
//		this.columnName = columnName;
//		return this;
//	}
}
