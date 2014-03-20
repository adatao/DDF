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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.SharkColumnVector;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;
import shark.api.JavaSharkContext;

@SuppressWarnings("serial")
public class VectorCovariance extends CExecutor {
	private String xDataContainerID;
	private String yDataContainerID;
	
	public VectorCovariance setxDataContainerID(String xDataContainerID) {
		this.xDataContainerID = xDataContainerID;
		return this;
	}

	public VectorCovariance setyDataContainerID(String yDataContainerID) {
		this.yDataContainerID = yDataContainerID;
		return this;
	}

	public static Logger LOG = LoggerFactory.getLogger(VectorCovariance.class);

	static public class VectorCovarianceResult extends SuccessResult {
		Double covariance;

		public VectorCovarianceResult(double covariance) {
			this.covariance = covariance;
		}

		public Double getCovariance() {
			return covariance;
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		LOG.info("cov({}, {})", xDataContainerID, yDataContainerID);
		DataContainer dcX = sparkThread.getDataManager().get(xDataContainerID);
		DataContainer dcY = sparkThread.getDataManager().get(yDataContainerID);
		if (dcX.getType() != dcY.getType()) {
			return new FailResult().setMessage("data container type mismatch");
		}
		if (dcX.getType().equals(DataContainer.ContainerType.DataFrame)) {
			return new FailResult().setMessage("not implemented for this data container type:"+dcX.getType());
		} else if (dcX.getType().equals(DataContainer.ContainerType.SharkColumnVector)) {
			SharkColumnVector vx, vy;
			vx = (SharkColumnVector) dcX;
			vy = (SharkColumnVector) dcY;
			if (! vx.getTableName().equals(vy.getTableName())) {
				return new FailResult().setMessage("can only calculate covariance between columns vector from the same table");
			}
			JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
			List<String> res = sc.sql(String.format("select covar_samp(%s, %s) from %s", vx.getColumn(), vy.getColumn(), vx.tableName));
			Double covar = Double.parseDouble(res.get(0));
			return new VectorCovarianceResult(covar);
		} else {
			return new FailResult().setMessage("invalid data container type for operation: "+dcX.getType());
		}
	}
}
