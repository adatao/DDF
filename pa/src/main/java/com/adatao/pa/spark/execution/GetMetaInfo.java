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

import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class GetMetaInfo extends CExecutor {
	private String dataContainerID;

	static public class GetMetaInfoResult extends SuccessResult {
		MetaInfo[] metaInfo;

		public GetMetaInfoResult setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataManager dm = sparkThread.getDataManager();
		DataContainer dc = dm.get(dataContainerID);
		MetaInfo[] metaInfo = dc.getMetaInfo();
		return new GetMetaInfoResult().setMetaInfo(metaInfo);
	}

	public GetMetaInfo setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}

}
