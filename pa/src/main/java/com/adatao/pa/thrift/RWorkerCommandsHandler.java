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
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;

public class RWorkerCommandsHandler implements RCommands.Iface {
	public static Logger LOG = LoggerFactory.getLogger(RWorkerCommandsHandler.class);
	final SparkThread sparkThread;
	final int uiPort;
	final int driverPort;
	final String clientID;
	final String sessionID;
	final SessionManager sessionManager;
	
	public RWorkerCommandsHandler(int uiPort, int driverPort, String clientID, String sessionID, SessionManager sessionManager) 
			throws FileNotFoundException, TException, InterruptedException {
		this.uiPort = uiPort;
		this.driverPort = driverPort;
		this.clientID = clientID;
		this.sessionID = sessionID;
		this.sessionManager = sessionManager;
		
		sparkThread = createSparkThread();
		sparkThread.start();
		
		/**
		 * when sparkThread started for the first time,
		 * it will push a message to resultQueue, so we have to get it out 
		 * this result does not have to send back to client because 
		 * the client who start this Worker already return a same result
		 */
		sparkThread.getResultQueue().take();
	}

	public JsonResult sendJsonCommand(JsonCommand cmd) throws TException, InterruptedException {
		if (!cmd.getSid().equals(sessionID)) {
			return new JsonResult().setResult(new FailResult()
				.setMessage(String.format("SessionID mismatch: got %s, expected %s", cmd.getSid(), sessionID)));
		}

		sparkThread.setLatestCommandTime(new Date());
		ArrayBlockingQueue<Object> cmdQueue = sparkThread.getCommandQueue();
		ArrayBlockingQueue<Object> resQueue = sparkThread.getResultQueue();
		LOG.info("Sending to SparkThread command: " + cmd);
		cmdQueue.put(cmd);
		JsonResult res = (JsonResult) resQueue.take();
		res.setSid(cmd.sid);
		LOG.info("Returning result: " + res);
		return res;
	}

	public SparkThread createSparkThread() throws TException, FileNotFoundException {
		ArrayBlockingQueue<Object> cmdQueue = new ArrayBlockingQueue<Object>(1);
		ArrayBlockingQueue<Object> resQueue = new ArrayBlockingQueue<Object>(1);
		return ((SparkThread) new SparkThread(cmdQueue, resQueue).setClientID(clientID)
				.setSessionManager(sessionManager).setSessionID(sessionID))
				.setDriverPort(driverPort).setUiPort(uiPort);
	}

	@Override
	public JsonResult execJsonCommand(JsonCommand cmd) throws TException {
		LOG.info("Got command from client: " + cmd.toString());
		try {
			return sendJsonCommand(cmd);
		} catch (InterruptedException e) {
			LOG.info("Got interrupted!");
			return new JsonResult().setResult(new FailResult().setMessage(e.getMessage()));
		}
	}
}
