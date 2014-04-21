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

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import org.apache.spark.api.java.JavaRDD;
import shark.api.JavaSharkContext;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.SharkUtils;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;
import com.adatao.pa.spark.Utils;


public class SampleDataFrame implements IExecutor {
	String dataContainerID;
	int size = 1000;
	double percent = 100.0;
	boolean replace = false;
	int seed = 1;
	boolean getPercent = true;

	public String getDataContainerID() {
		return dataContainerID;
	}
	public SampleDataFrame setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return(this);
	}
	public int getSize() {
		return size;
	}
	public SampleDataFrame setSize(int size) {
		this.size = size;
		return(this);
	}
	public double getPercent() {
		return percent;
	}
	public SampleDataFrame setPercent(double percent) {
		this.percent = percent;
		return(this);
	}
	public boolean isReplace() {
		return replace;
	}
	public SampleDataFrame setReplace(boolean replace) {
		this.replace = replace;
		return(this);
	}
	public int getSeed() {
		return seed;
	}
	public SampleDataFrame setSeed(int seed) {
		this.seed = seed;
		return(this);
	}
	public boolean isGetPercent() {
		return getPercent;
	}
	public SampleDataFrame setGetPercent(boolean getPercent) {
		this.getPercent = getPercent;
		return(this);
	}

	static public class SampleDataFramePercentResult extends SuccessResult {
		String dataContainerID;
		MetaInfo[] metaInfo;

    public SampleDataFramePercentResult(String dcID, MetaInfo[] metaInfos) {
      dataContainerID = dcID;
      metaInfo = metaInfos;
    }

		public SampleDataFramePercentResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

		public String getDataContainerID() {
			return dataContainerID;
		}

		public SampleDataFramePercentResult setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}
	}

	static public class SampleDataFrameSizeResult extends SuccessResult {
		String dataContainerID;
		List<Object[]> data;

		public SampleDataFrameSizeResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}
		public SampleDataFrameSizeResult setData(List<Object[]> data) {
			this.data = data;
			return this;
		}
		public List<Object[]> getData() {
			return data;
		}
	}
	@Override
	public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
		DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(Utils.getDDFNameFromDataContainerID(dataContainerID));

    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF " + dataContainerID, null);
    }

    //JavaRDD<Object[]> dataTable = null;

		if (getPercent) {
      /*
			JavaRDD<Object[]> data = dataTable.sample(replace, percent, seed);
			DataFrame tdf;
			tdf = new DataFrame(df.getMetaInfo(), data);
			JavaSharkContext jsc =  (JavaSharkContext) sparkThread.getSparkContext();
			SharkDataFrame sdf= SharkUtils.createSharkDataFrame(tdf, jsc);
			String uid = dm.add(sdf);
			// TODO Auto-generated method stub

			return new SampleDataFramePercentResult()
			.setDataContainerID(uid)
			.setMetaInfo(tdf.getMetaInfo());    */
      DDF sampleDDF = ddf.Views.getRandomSample(percent, replace, seed);
      try{
        MetaInfo[] metaInfos = Utils.generateMetaInfo(sampleDDF.getSchema());
        String dcID = Utils.getDataContainerID(sampleDDF);
        return new SampleDataFramePercentResult(dcID, metaInfos);
      } catch(DDFException e) {
        throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e.getCause());
      }
    } else {

			int MAX_SAMPLE_SIZE = Integer.parseInt(System.getProperty("sample.max.size", "10000000"));
			if(size > MAX_SAMPLE_SIZE) {
				throw new AdataoException(AdataoExceptionCode.ERR_SAMPLE_MAX_SIZE, String.format("Sample size exceeds limit: size %d, MAX_SAMPLE_SIZE %d", size, MAX_SAMPLE_SIZE), null);
			}
      /*
			List<Object[]> data = dataTable.takeSample(replace, size, seed);
			
			if(data != null && data.size() > 0) {
				return new SampleDataFrameSizeResult()
				.setDataContainerID(dataContainerID)
				.setData(data);  */
			List<Object[]> data = ddf.Views.getRandomSample(size, replace, seed);
      if(data != null && data.size() > 0) {
        return new SampleDataFrameSizeResult().setDataContainerID(dataContainerID).setData(data);
      }
			else {
				//provide metaInfo for easy debugging. At this step it's certain that df is not null
				if(ddf != null && ddf.getSchema() != null) {
          try{
            MetaInfo[] metaInfos = Utils.generateMetaInfo(ddf.getSchema());
            throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Fail to get sample data for dataframe: [MetaInfo] = " + metaInfos, null);
          } catch(DDFException e) {
            throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e.getCause());
          }
				}
				throw AdataoException.defaultException;
			}
		}
	}
}
