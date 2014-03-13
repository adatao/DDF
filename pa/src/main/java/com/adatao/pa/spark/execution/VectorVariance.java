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


import shark.api.JavaSharkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SharkQueryUtils;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.SharkColumnVector;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;
import com.adatao.ML.types.TJsonSerializable;
import com.adatao.ML.types.TJsonSerializable$class;
import java.io.Serializable;
import java.util.List;
/**
 * @author ngonpham Get variance for both vector and dataframe
 */
@SuppressWarnings("serial")
public class VectorVariance implements IExecutor, Serializable {
	private String dataContainerID;

	static public class VectorVarianceResult extends SuccessResult implements TJsonSerializable {
		String dataContainerID;
		double variance, stddev;
		String clazz;
		
		public String getDataContainerID() {
			return dataContainerID;
		}

		public VectorVarianceResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

		public VectorVarianceResult setVariance(double variance) {
			this.variance = variance;
			return this;
		}

		public double getVariance() {
			return variance;
		}

		public VectorVarianceResult setStddev(double stddev) {
			this.stddev = stddev;
			return this;
		}

		public double getStdDev() {
			return stddev;
		}
		
		public String clazz() {
	      return clazz;
	    }

		public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
		  clazz = Aclass;
		}

		@Override
		public TJsonSerializable fromJson(String jsonString) {
		  return TJsonSerializable$class.fromJson(this, jsonString);
		}
	}

	public VectorVariance setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
		DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
		if (dc.getType().equals(DataContainer.ContainerType.DataFrame)) {
			DataFrame df = (DataFrame) sparkThread.getDataManager().get(dataContainerID);
			JavaRDD<Object[]> filteredRdd = df.getRDD().filter(
					new Function<Object[], Boolean>() {
						@Override
						public Boolean call(Object[] t) throws Exception {
							return (t[0] != null)
									&& ((t[0] instanceof Integer) || (!Double
									.isNaN((Double) t[0])));
						}
					});
			JavaDoubleRDD rdd = filteredRdd.map(new DoubleFunction<Object[]>() {
				@Override
				public Double call(Object[] t) throws Exception {
					if (t[0] instanceof Integer)
						return (double) ((Integer) t[0]).intValue();
					else
						return (Double) t[0];
				}
			});
			StatCounter stat = rdd.stats();
			double variance = stat.sampleVariance();
			return new VectorVarianceResult().setDataContainerID(dataContainerID)
					.setVariance(variance)
					.setStddev(Math.sqrt(variance));
		} else if (dc.getType().equals(DataContainer.ContainerType.SharkColumnVector)) {
			SharkColumnVector v = (SharkColumnVector) dc;
			JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
			Double variance = SharkQueryUtils.sql2Double(sc, String.format("select var_samp(%s) from %s", v.getColumn(), v.tableName),
					AdataoExceptionCode.ERR_GENERAL);
			return new VectorVarianceResult().setDataContainerID(dataContainerID)
					.setVariance(variance)
					.setStddev(Math.sqrt(variance));
		} else {
			throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_CONTAINER_TYPE, 
					String.format("the variance function argument must be a vector: found type %s", dc.getType()), null);
		}
	}
}
