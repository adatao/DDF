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
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class FetchRows extends CExecutor {
	private String dataContainerID;
	int limit = 1000;
	
	static public class FetchRowsResult extends SuccessResult {
		String dataContainerID;
		List<Object[]> data;
		
		public FetchRowsResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}
		public FetchRowsResult setData(List<Object[]> data) {
			this.data = data;
			return this;
		}
		public List<Object[]> getData() {
			return data;
		}
	}
	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataManager dm = sparkThread.getDataManager();
		DataContainer dc = dm.get(dataContainerID);

		List<Object[]> data;
		data = dc.getRDD().take(limit);
		//Iterator<Object[]> it = data.iterator();
		//while (it.hasNext()) {
		//	System.out.println(Arrays.toString(it.next()));
		//}
		return new FetchRowsResult().setDataContainerID(dataContainerID).setData(data);
	}

	public FetchRows setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}
	public FetchRows setLimit(int limit) {
		this.limit = limit;
		return this;
	}
}
