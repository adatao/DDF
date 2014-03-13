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

import org.apache.thrift.TException;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands.Client;
import com.google.gson.Gson;

public class ClientUtils {
	Client client;
	String sid;

	public ExecutorResult execCmd(IExecutor cmd) throws TException, ClassNotFoundException {
		Gson gson = new Gson();

		JsonCommand jcmd = new JsonCommand().setCmdName(cmd.getClass().getSimpleName())
				.setSid(sid)
				.setParams(gson.toJson(cmd));
		
		JsonResult jres = client.execJsonCommand(jcmd);
		return (ExecutorResult) gson.fromJson(jres.getResult(), Class.forName(cmd.getClass().getName()
				+"$"+cmd.getClass().getSimpleName()+"Result"));
	}

	public ExecutorResult execCmd(String cmdName) throws TException, ClassNotFoundException {
		Gson gson = new Gson();

		JsonCommand jcmd = new JsonCommand().setCmdName(cmdName).setSid(sid);
		
		JsonResult jres = client.execJsonCommand(jcmd);
		return (ExecutorResult) gson.fromJson(jres.getResult(), Class.forName("adatao.bigr.spark.types.ExecutorResult"));
	}

	
	public ClientUtils setClient(Client client) {
		this.client = client;
		return this;
	}

	public ClientUtils setSid(String sid) {
		this.sid = sid;
		return this;
	}
}
