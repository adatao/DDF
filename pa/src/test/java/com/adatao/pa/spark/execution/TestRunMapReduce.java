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
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.execution.RunMapReduce.RunMapReduceResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.spark.types.ExecutionResult;

public class TestRunMapReduce extends BaseTest {
	public static Logger LOG = LoggerFactory.getLogger(TestRunMapReduce.class);

	@Ignore
	public void test() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
//    Gson gson = new Gson();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);
		Thread.sleep(5000);

		// RunMapReduce
		String jsCreateVectors = String
				.format("{file: \"resources/wordcount.txt\", map: \"%s\", reduce: \"%s\", combine: \"%s\", count: %d}",
						"function(k,v) { lapply( strsplit(x=v, split=',')[[1]], function(w) list(k=w, v=1) ) }",
						"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
						"function(k, values) { res <- 0 \n n <- length(values) \n for (i in 1:n) {\n res <- res + as.integer(values[i]) \n } \n as.character(res) \n }",
						10);
		cmd.setSid(sid).setCmdName("RunMapReduce").setParams(jsCreateVectors);
		res = client.execJsonCommand(cmd);
		RunMapReduceResult rmr = ExecutionResult.fromJson(res.getResult(), RunMapReduceResult.class).result();
		LOG.info("RunMapReduce Result = " + rmr.getResult());
		assertTrue(rmr.getResult().startsWith("a,3"));

		cmd.setCmdName("disconnect").setSid(sid).setParams(null);
		res = client.execJsonCommand(cmd);
		String newSid = res.sid;
		assert (newSid.equals(sid));
	}
}
