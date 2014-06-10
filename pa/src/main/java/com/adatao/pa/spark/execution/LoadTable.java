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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FastArrayIterator;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;


@SuppressWarnings("serial")
public class LoadTable implements IExecutor {

	// { API variables

	private String fileURL;

	private boolean hasHeader = false;

	private String separator = " ";

	private String fileType;

	/**
	 * If true, always use double in preference to integer. This is useful to avoid repeated casting of ints into
	 * doubles for, e.g., linear regression
	 */
	private boolean doPreferDouble = true;

	/**
	 * If not null, then parse only these columns. Default = all columns.
	 */
	private int[] columns;

	/**
	 * If one of the columns parses to null or NaN, drop the whole line. Default = don't drop. The way we support this
	 * is as follows: first, if we detect a null or NaN in a row, we set the entire row value to null. Then in the
	 * partition iterator returned, by using SerializableObjectArrayIterator, it will skip all null row values
	 * automatically.
	 */
	private boolean doDropNaNs;

	private int sampleSize = 2;



	public String getFileURL() {
		return fileURL;
	}


	public LoadTable setFileURL(String fileURL) {
		this.fileURL = fileURL;
		return this;
	}


	public boolean isHasHeader() {
		return hasHeader;
	}


	public LoadTable setHasHeader(boolean hasHeader) {
		this.hasHeader = hasHeader;
		return this;
	}


	public String getSeparator() {
		return separator;
	}


	public LoadTable setSeparator(String separator) {
		this.separator = separator;
		return this;

	}


	public String getFileType() {
		return fileType;
	}


	public LoadTable setFileType(String fileType) {
		this.fileType = fileType;
		return this;

	}


	public boolean isDoPreferDouble() {
		return doPreferDouble;
	}


	public LoadTable setDoPreferDouble(boolean doPreferDouble) {
		this.doPreferDouble = doPreferDouble;
		return this;
	}


	public int[] getColumns() {
		return columns;
	}


	public LoadTable setColumns(int[] columns) {
		this.columns = columns.clone();
		return this;

	}


	public boolean isDoDropNaNs() {
		return doDropNaNs;
	}


	public LoadTable setDoDropNaNs(boolean doDropNaNs) {
		this.doDropNaNs = doDropNaNs;
		return this;

	}


	public int getSampleSize() {
		return sampleSize;
	}


	public LoadTable setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
		return this;

	}

	public static Logger LOG = LoggerFactory.getLogger(LoadTable.class);

	static public class LoadTableResult extends SuccessResult {

		String dataContainerID;
		MetaInfo[] metaInfo;

		public String getDataContainerID() {
			return dataContainerID;
		}

		public LoadTableResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

		public LoadTableResult setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.adatao.pa.spark.execution.Executor#run()
	 */
	@Override
	public ExecutorResult run(SparkThread sparkThread) {
	  DDFManager ddfManager = sparkThread.getDDFManager();
	  try {
  	  DDF ddf = ddfManager.loadTable(fileURL, separator);
  	  
  	  String ddfName = ddfManager.addDDF(ddf);
      LOG.info("DDF Name: " + ddfName);
  	  String dataContainerID = ddfName.substring(15).replace("_", "-");;
  		// LOG.info("Meta info: " + Arrays.toString(metaInfo));
  
  		return new LoadTableResult().setDataContainerID(dataContainerID).setMetaInfo(generateMetaInfo(ddf.getSchema()));
	  } catch (Exception e) {
	    return null;
	  }
	}
	
	public static MetaInfo[] generateMetaInfo(Schema schema) {
    List<Column> columns = schema.getColumns();
    MetaInfo[] metaInfo = new MetaInfo[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
    }
    return metaInfo;
  }

	@Override
	public String toString() {
		return "LoadTable [fileURL=" + fileURL + ", hasHeader=" + hasHeader + ", separator=" + separator + ", type="
				+ fileType + "]";
	}
}
