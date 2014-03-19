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

import java.io.StringWriter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

/**
 * @author ngonpham Hadoop FsShell command
 */
@SuppressWarnings("serial")
public class HadoopFsShell extends CExecutor {
	private String command;
	private String params;

	public static Logger LOG = LoggerFactory.getLogger(HadoopFsShell.class);

	public HadoopFsShell(String command, String params) {
		super();
		this.command = command;
		this.params = params;
	}

	static public class HadoopFsShellResult extends SuccessResult {
		String result;

		public HadoopFsShellResult(String result) {
			super();
			this.result = result;
		}

		public String getResult() {
			return result;
		}

		public void setResult(String result) {
			this.result = result;
		}

	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
		String HADOOP_HOME = System.getenv("HADOOP_HOME");
		if (HADOOP_HOME == null)
			throw new AdataoException(AdataoExceptionCode.ERR_HADOOPHOME_UNDEF, null);
		if (!HADOOP_HOME.endsWith("/"))
			HADOOP_HOME += "/";
		String result = "";
		try {
			Process p = Runtime.getRuntime().exec(HADOOP_HOME + "bin/hadoop fs -" + command + " " + params);
			p.waitFor();
			StringWriter writer = new StringWriter();
			IOUtils.copy(p.getInputStream(), writer, "UTF-8");
			result = writer.toString();
			LOG.info(String.format("HadoopFsShell result: %s", result));
		} catch (Exception e) {
			throw new AdataoException(AdataoExceptionCode.ERR_HDFS,e);
//			return new FailResult().setMessage(e.getMessage());
		}
		return new HadoopFsShellResult(result);
	}
}
