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
import adatao.bigr.spark.types.ExecutionResult;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.execution.HadoopFsShell;
import com.adatao.pa.spark.execution.HadoopFsShell.HadoopFsShellResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestHadoopFsShell extends BaseTest {
	public static Logger LOG = LoggerFactory.getLogger(TestHadoopFsShell.class);

	@Ignore
	public void test() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);

		// Test ls
		HadoopFsShell hcmd = (HadoopFsShell) new HadoopFsShell("ls", "/");
		LOG.info(gson.toJson(hcmd));
		cmd.setSid(sid).setCmdName("HadoopFsShell").setParams(gson.toJson(hcmd));
		res = client.execJsonCommand(cmd);

		HadoopFsShellResult result = ExecutionResult.fromJson(res.getResult(), HadoopFsShellResult.class).result();
		LOG.info("HadoopFsShell result: " + result.getResult());
		assertEquals(true, result.getResult().indexOf("\"success\":false") < 0
				&& result.getResult().indexOf("Error") < 0);

		// Test tail
		// hcmd = (HadoopCommand) new HadoopCommand("tail",
		// "/Users/ngonpham/text.csv");
		// LOG.info(gson.toJson(hcmd));
		// cmd.setSid(sid).setCmdName("HadoopCommand").setParams(gson.toJson(hcmd));
		// res = client.execJsonCommand(cmd);
		//
		// result = gson.fromJson(res.getResult(), HadoopCommandResult.class);
		// LOG.info("HadoopCommand result: " + result.getResult());
	}
}
