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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.Connect;
import com.adatao.pa.spark.execution.ExecutionResultUtils;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class RCommandsHandler implements RCommands.Iface {
	public static Logger LOG = LoggerFactory.getLogger(RCommandsHandler.class);

	SessionManager sessionManager;

	static long EXPIRED_TIME = 30 * 60000; // 30 minutes

	// lock for the Connect command, we want only one Connect command 
	// is being executing at any moment in time
	private static final ReentrantLock connectLock = new ReentrantLock();
	
	public RCommandsHandler(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
	}

	public JsonResult sendJsonCommand(JsonCommand cmd) throws TException, JsonSyntaxException, ClassNotFoundException {
		if (!sessionManager.hasSession(cmd.getSid())) {
			return new JsonResult().setResult(new FailResult().setMessage("Session closed. Please reconnect!"));
		}

		SparkThread sparkThread = (SparkThread) sessionManager.getSessionThread(cmd.getSid());
		JsonResult res;
		try {
			res = new JsonResult().setResult(sparkThread.processJsonCommand1(cmd).toJson());
		} catch (Exception e) {
			LOG.error("Exception: ", e);
			Exception processedException = ExecutionResultUtils.processException(e);
			return new JsonResult().setResult(new FailResult().setMessage(processedException.getMessage()));			
		}
		LOG.info("Returning result: " + res);
		return res;
	}

	public JsonResult disconnect(JsonCommand cmd) throws TException, FileNotFoundException {
		LOG.info("Disconnect session " + cmd.getSid());
//		sessionManager.stopSession(cmd.getSid());
		return new JsonResult().setSid(cmd.getSid());
	}

	@Override
	public JsonResult execJsonCommand(JsonCommand cmd) throws TException {
		LOG.info("execJsonCommand: " + cmd);
		if (cmd.getCmdName().toLowerCase().equals("connect")) {
			// this is a newer way to do connect
			try {
				Gson gson = new GsonBuilder()
					.excludeFieldsWithoutExposeAnnotation()
					.create();
				
				Connect connect;
				if (cmd.params == null || cmd.params.equals("")) {
					connect = new Connect().setSessionManager(sessionManager);
				} else {
					connect = gson.fromJson(cmd.params, Connect.class).setSessionManager(sessionManager);
				}
				
				connectLock.lock();
				JsonResult res = connect.run();
				connectLock.unlock();
				
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
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		} else {
			try {
				return sendJsonCommand(cmd);
			} catch (JsonSyntaxException e) {
				LOG.info("Got interrupted!");
				return new JsonResult().setResult(new FailResult().setMessage(e.getMessage()));
			} catch (ClassNotFoundException e) {
				LOG.info("Got interrupted!");
				return new JsonResult().setResult(new FailResult().setMessage(e.getMessage()));
			}
		}

	}
}
