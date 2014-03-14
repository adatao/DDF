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

package com.adatao.pa.thrift;

import java.io.FileNotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.execution.MultiContextConnect;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RMultiContextCommandsHandler implements RCommands.Iface {
	public static Logger LOG = LoggerFactory.getLogger(RMultiContextCommandsHandler.class);
	
	SessionManager sessionManager;
	
	public RMultiContextCommandsHandler(SessionManager sessionManager){
		this.sessionManager = sessionManager;
	}
	
	public JsonResult disconnect(JsonCommand cmd) throws TException, FileNotFoundException {
		LOG.info("Disconnect session " + cmd.getSid());
		sessionManager.stopSession(cmd.getSid());		
		return new JsonResult().setSid(cmd.getSid());
	}

	@Override
	public JsonResult execJsonCommand(JsonCommand cmd) throws TException {
		if (cmd.getCmdName().toLowerCase().equals("connect")) {
			try {
				Gson gson = new GsonBuilder()
					.excludeFieldsWithoutExposeAnnotation()
					.create();
				LOG.info("Get connect command: "+cmd.params);
				
				MultiContextConnect connect;
				if (cmd.params == null){ 
					connect = new  MultiContextConnect();
				} else {
					connect = gson.fromJson(cmd.params, MultiContextConnect.class);
				}
				connect.setSessionManager(sessionManager).setHost("localhost");
				JsonResult res = connect.run();
				LOG.info(res.toString());
				return res;
			} catch (Exception e) {
				LOG.info("Exception: ", e);
				return new JsonResult().setResult(new FailResult().setMessage(e.getMessage()));
			}
		} else if (cmd.getCmdName().toLowerCase().equals("disconnect")) {
			try {
				return disconnect(cmd);
			} catch (FileNotFoundException e) {
				LOG.info("Exception: ", e);
				return new JsonResult().setResult(new FailResult().setMessage(e.getMessage()));
			}
		} else {
			return new JsonResult().setResult(new FailResult().setMessage("BigRServer does not handle this command in multiple context mode"));
		}

	}
}
