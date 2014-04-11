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

import static org.junit.Assert.assertTrue;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult;
import com.adatao.pa.spark.execution.LoadTable.LoadTableResult;
import com.adatao.pa.spark.execution.RunMapReduceTextFile.RunMapReduceTextFileResult;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;

public class TestRunMapReduceTextFile extends BaseTest {
	public static Logger LOG = LoggerFactory.getLogger(TestRunMapReduceTextFile.class);

	public void writeToFile(String fileName, String data) throws IOException {
		if (fileName.lastIndexOf("/") > 0) {
			String dirPath = fileName.substring(0, fileName.lastIndexOf("/"));
			File dir = new File(dirPath);
			if (!dir.exists())
				dir.mkdirs();
		}
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "UTF8"));
		out.write(data);
		out.close();
	}

	@Ignore
	public void test() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		Gson gson = new Gson();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);
		Thread.sleep(5000);

		String TEMP_FILE = "/tmp/wordcount-input.txt";
		writeToFile(TEMP_FILE, "a,a,a");

		// RunMapReduce
		String jsCreateVectors = String
				.format("{input: \"file:///tmp/wordcount-input.txt\", output: \"file:///tmp/wordcount-output\", map: \"%s\", reduce: \"%s\", combine: \"%s\", count: %d}",
						"function(k,v) { lapply( strsplit(x=v, split=',')[[1]], function(w) list(k=w, v=1) ) }",
						"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
						"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
						10);
		cmd.setSid(sid).setCmdName("RunMapReduceTextFile").setParams(jsCreateVectors);
		res = client.execJsonCommand(cmd);
		RunMapReduceTextFileResult rmr = ExecutionResult.fromJson(res.getResult(), RunMapReduceTextFileResult.class).result();
		LOG.info("RunMapReduce Result = " + rmr.getResult());
		assertTrue(rmr.getResult().equals("file:///tmp/wordcount-output"));
		
		//load Table 
		LoadTable loadTbl = (LoadTable) new LoadTable()
        .setFileURL("/tmp/wordcount-output")
        .setHasHeader(false)
        .setSeparator(",");

		LOG.info(gson.toJson(loadTbl));
		cmd.setSid(sid)
		        .setCmdName("LoadTable")
		        .setParams(gson.toJson(loadTbl));
		
		res = client.execJsonCommand(cmd);
		
		LoadTableResult ltr = ExecutionResult.fromJson(res.getResult(), LoadTableResult.class).result();
		
		cmd.setSid(sid)
		.setCmdName("FetchRows")
		.setParams(String.format("{dataContainerID: %s}",ltr.getDataContainerID()));
		res = client.execJsonCommand(cmd);
		FetchRowsResult frr = ExecutionResult.fromJson(res.getResult(), FetchRowsResult.class).result();
		Object[] r = frr.getData().get(0).split("\\t");
		String r11 = (String)r[0];
		Double r12 = (Double)r[1];
		LOG.info(r11 + " " + r12);
		assertTrue(r11.equals("a"));// && r12.equals("3"));

		cmd.setCmdName("disconnect").setSid(sid).setParams(null);
		res = client.execJsonCommand(cmd);
		String newSid = res.sid;
		assert (newSid.equals(sid));
		new File(TEMP_FILE).delete();
	}
}
