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
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SharkQueryUtils;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import shark.api.JavaSharkContext;

@SuppressWarnings("serial")
public class NRow extends CExecutor {
	private String dataContainerID;

	public static Logger LOG = LoggerFactory.getLogger(NRow.class);

	static public class NRowResult extends SuccessResult {
		public long nrow;

		public NRowResult(long nrow) {
			this.nrow = nrow;
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
		DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
		if (dc.getType() == DataContainer.ContainerType.DataFrame) {
			DataFrame df = (DataFrame) dc;
			return new NRowResult(df.getRDD().count());
		} else if (dc.getType() == DataContainer.ContainerType.SharkDataFrame || dc.getType() == DataContainer.ContainerType.SharkColumnVector) {
			// could still use rdd.count(),
			// but let's try a Hive UDAF!
			SharkDataFrame df = (SharkDataFrame) dc;
			JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
			Long count = SharkQueryUtils.sql2Long(sc, "select count(*) from " + df.tableName, AdataoExceptionCode.ERR_GENERAL);
			return new NRowResult(count);
		} else {
			throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_CONTAINER_TYPE, 
					String.format("unssuported container type: %s", dc.getType()), null);
		}
	}

	public NRow setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}

}
