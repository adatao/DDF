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

import java.io.Serializable;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.SharkColumnVector;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.NumericHistogram;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import shark.api.JavaSharkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings({ "serial" })
public class VectorHistogram implements IExecutor, Serializable {
	public static Logger LOG = LoggerFactory.getLogger(VectorHistogram.class);
	private String dataContainerID;
	private int numBins;

	public VectorHistogram setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}

	public void setNumBins(int numBins) {
		this.numBins = numBins;
	}

	static public class HistogramBin {
		private double x; // Bin center
		private double y; // Bin weight

		public double getX() {
			return x;
		}

		public void setX(double x) {
			this.x = x;
		}

		public double getY() {
			return y;
		}

		public void setY(double y) {
			this.y = y;
		}

	}

	static public class VectorHistogramResult extends SuccessResult {
		String dataContainerID;
		List<HistogramBin> histogramBins;

		public List<HistogramBin> getHistogramBins() {
			return histogramBins;
		}

		public VectorHistogramResult setHistogramBins(
				List<HistogramBin> histogramBins) {
			this.histogramBins = histogramBins;
			return this;
		}

		public String getDataContainerID() {
			return dataContainerID;
		}

		public VectorHistogramResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
		if (dc.getType().equals(DataContainer.ContainerType.SharkColumnVector)) {
			SharkColumnVector v = (SharkColumnVector) dc;
			JavaSharkContext sc = (JavaSharkContext) sparkThread
					.getSparkContext();
			List<String> res = sc.sql(String.format(
					"select histogram_numeric(%s,%s) from %s", v.getColumn(),
					numBins, v.tableName));
			List<HistogramBin> bins = new ArrayList<VectorHistogram.HistogramBin>();
			Gson gson = new Gson();
			for (String rs : res) {
				Type typeOfT = new TypeToken<List<HistogramBin>>() {
				}.getType();
				List<HistogramBin> tmpbins = gson
						.fromJson((String) rs, typeOfT);
				for (HistogramBin bin : tmpbins)
					bins.add(bin);
			}
			return new VectorHistogramResult().setDataContainerID(
					dataContainerID).setHistogramBins(bins);
		} else if (dc.getType().equals(DataContainer.ContainerType.DataFrame)) {
			DataFrame df = (DataFrame) sparkThread.getDataManager().get(
					dataContainerID);
			JavaRDD<Object[]> filteredRdd = df.getRDD().filter(
					new Function<Object[], Boolean>() {
						@Override
						public Boolean call(Object[] t) {
							return (t[0] != null)
									&& ((t[0] instanceof Integer) || (!Double
											.isNaN((Double) t[0])));
						}
					});
			NumericHistogram nh = filteredRdd
					.mapPartitions(
							new FlatMapFunction<Iterator<Object[]>, NumericHistogram>() {
								@Override
								public Iterable<NumericHistogram> call(
										Iterator<Object[]> arg0)
										{
									List<NumericHistogram> res = new ArrayList<NumericHistogram>(
											1);
									NumericHistogram nh = new NumericHistogram();
									nh.allocate(numBins);
									while (arg0.hasNext()) {
										nh.add((Double) arg0.next()[0]);
									}
									res.add(nh);
									return res;
										}
							})
							.reduce(new Function2<NumericHistogram, NumericHistogram, NumericHistogram>() {
								@Override
								public NumericHistogram call(NumericHistogram arg0,
										NumericHistogram arg1) {
									arg0.merge(arg1.serialize());
									return arg0;
								}
							});
			List<HistogramBin> bins = new ArrayList<VectorHistogram.HistogramBin>();
			ArrayList<Double> arr = nh.serialize();
			for (int i = 1; i < arr.size(); i += 2) {
				HistogramBin hb = new HistogramBin();
				hb.setX(arr.get(i));
				hb.setY(arr.get(i + 1));
				bins.add(hb);
			}
			return new VectorHistogramResult().setDataContainerID(
					dataContainerID).setHistogramBins(bins);
		} else {
			return new FailResult()
			.setMessage("invalid data container type for operation: "
					+ dc.getType());
		}
	}
}
