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

import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult;
import com.adatao.pa.spark.execution.LoadTable.LoadTableResult;
import com.adatao.pa.spark.execution.SampleDataFrame.SampleDataFramePercentResult;
import com.adatao.pa.spark.execution.SampleDataFrame.SampleDataFrameSizeResult;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;

public class TestSampleDataFrame extends BaseTest {
	public static Logger LOG = LoggerFactory.getLogger(TestSampleDataFrame.class);

	@Ignore
	public void test() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		Gson gson = new Gson();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);
		try {
		LoadTable loadTbl = (LoadTable) new LoadTable().setFileURL("resources/table_noheader_noNA.csv")
				.setHasHeader(false).setSeparator(",");

		LOG.info(gson.toJson(loadTbl));
		cmd.setSid(sid).setCmdName("LoadTable").setParams(gson.toJson(loadTbl));
		res = client.execJsonCommand(cmd);

		LoadTableResult result = ExecutionResult.fromJson(res.getResult(), LoadTableResult.class).result();
		LOG.info("LoadTable result: " + Arrays.toString(result.getMetaInfo()));
		String dcID = result.getDataContainerID();

		res = client.execJsonCommand(cmd
				.setSid(sid)
				.setCmdName("SampleDataFrame")
				.setParams(
						String.format("{dataContainerID: %s," + "size: 2," + "replace: false," + "getPercent: false}",
								dcID)));
		SampleDataFrameSizeResult sdfsr = ExecutionResult.fromJson(res.getResult(), SampleDataFrameSizeResult.class).result();

		assertEquals(true, sdfsr.isSuccess());
		Double r11 = (Double) sdfsr.getData().get(0)[0];
		LOG.info("r11: " + r11.doubleValue());
		assertEquals(r11.doubleValue(), 8346.34, 0.01);

		res = client.execJsonCommand(cmd
				.setSid(sid)
				.setCmdName("SampleDataFrame")
				.setParams(
						String.format("{dataContainerID: %s," + "percent: 0.5," + "replace: false,"
								+ "getPercent: true}", dcID)));
		SampleDataFramePercentResult sdfpr = ExecutionResult.fromJson(res.getResult(), SampleDataFramePercentResult.class).result();
		assertEquals(true, sdfpr.isSuccess());

		cmd.setSid(sid).setCmdName("FetchRows")
				.setParams(String.format("{dataContainerID: %s}", sdfpr.getDataContainerID()));
		res = client.execJsonCommand(cmd);
		FetchRowsResult frr = ExecutionResult.fromJson(res.getResult(), FetchRowsResult.class).result();
		r11 = (Double) frr.getData().get(0)[0];
		LOG.info("r11: " + r11.doubleValue());
		assertEquals(r11.doubleValue(), 123.23, 0.01);
		} finally {
			client.execJsonCommand(new JsonCommand().setCmdName("disconnect").setSid(sid));
		}
	}
	
	@Ignore
	public void testShark() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
//		Gson gson = new Gson();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);
		
		try {
			assert runSQLCmd(sid, "set shark.test.data.path=" + "resources").isSuccess();
			assert runSQLCmd(sid, "drop table if exists mtcars").isSuccess();
			assert runSQLCmd(sid, "CREATE TABLE mtcars ("
			+"mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb int"
			+") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '").isSuccess();
			assert runSQLCmd(sid, "LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/mtcars' INTO TABLE mtcars").isSuccess();

			Sql2DataFrame.Sql2DataFrameResult df = runSQL2RDDCmd(sid, "SELECT * FROM mtcars", true);
			assert df.isSuccess();

			String dcID = df.dataContainerID;
			LOG.info("Got dataContainerID = " + dcID);

			res = client.execJsonCommand(cmd
					.setSid(sid)
					.setCmdName("SampleDataFrame")
					.setParams(
							String.format("{dataContainerID: %s," + "size: 2," + "replace: false," + "getPercent: false}",
									dcID)));
			SampleDataFrameSizeResult sdfsr = ExecutionResult.fromJson(res.getResult(), SampleDataFrameSizeResult.class).result();

			assertEquals(true, sdfsr.isSuccess());
			Double r11 = (Double) sdfsr.getData().get(0)[0];
			LOG.info("r11: " + r11.doubleValue());
			assertEquals(r11.doubleValue(), 13.3, 0.01);
			
			res = client.execJsonCommand(cmd
					.setSid(sid)
					.setCmdName("SampleDataFrame")
					.setParams(
							String.format("{dataContainerID: %s," + "percent: 0.5," + "replace: false,"
									+ "getPercent: true}", dcID)));
			SampleDataFramePercentResult sdfpr = ExecutionResult.fromJson(res.getResult(), SampleDataFramePercentResult.class).result();
			assertEquals(true, sdfpr.isSuccess());

			cmd.setSid(sid).setCmdName("FetchRows")
					.setParams(String.format("{dataContainerID: %s}", sdfpr.getDataContainerID()));
			res = client.execJsonCommand(cmd);
			FetchRowsResult frr = ExecutionResult.fromJson(res.getResult(), FetchRowsResult.class).result();
			r11 = (Double) frr.getData().get(0)[0];
			LOG.info("r11: " + r11.doubleValue());
			assertEquals(r11.doubleValue(), 21, 0.01);
			
		} finally {
			client.execJsonCommand(new JsonCommand().setCmdName("disconnect").setSid(sid));
		}
	}	
}
