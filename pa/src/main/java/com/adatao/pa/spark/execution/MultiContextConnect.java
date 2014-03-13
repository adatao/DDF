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

import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import adatao.bigr.spark.MultiContextConnectResult;
import adatao.bigr.spark.MultiContextThread;
import adatao.bigr.spark.types.ExecutionResult;
import adatao.bigr.thrift.Session;
import adatao.bigr.thrift.SessionManager;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.annotations.Expose;

@SuppressWarnings("serial")
public class MultiContextConnect {
	public static Logger LOG = LoggerFactory.getLogger(MultiContextConnect.class);

	/**
	 * These are created during gson deserialization process
	 */
	@Expose private String clientID;
	@Expose private Boolean isShark;

	private String host;

	SessionManager sessionManager;

	public MultiContextConnect(){
		this.clientID = SessionManager.ANONYMOUS();
		this.isShark = true;
	}
	
	public JsonResult run() throws InterruptedException {
		LOG.info("CLIENT ID: " + clientID);
		if (sessionManager.hasClient(clientID)) {
			String sessionID = sessionManager.getSessionID(clientID);
			Session session = sessionManager.getSession(sessionID);
			// fixed, however, this is a error-prone way to construct the json string, :((((((((
			String res = String.format("{\"success\":true, \"result\":{\"host\":%s, \"thriftPort\":%s, \"uiPort\":%s, \"clientID\":\"%s\", \"sessionID\":\"%s\"}, \"resultType\":\"adatao.bigr.spark.MultiContextConnectResult\"}", 
					null, session.thriftPort(), session.uiPort(), session.clientID(), session.sessionID());
			
			return new JsonResult().setResult(res).setSid(sessionID);
		}

		ArrayBlockingQueue<ExecutionResult<MultiContextConnectResult>> resQueue 
			= new ArrayBlockingQueue<ExecutionResult<MultiContextConnectResult>>(1);

		MultiContextThread sparkThread = (MultiContextThread) new MultiContextThread(host, resQueue)
														.setClientID(clientID)
														.setSessionManager(sessionManager)
														.setShark(isShark);
		sparkThread.start();
		ExecutionResult<MultiContextConnectResult> res = resQueue.take();
		System.out.println(res.toJson());
		return new JsonResult().setResult(res.toJson()).setSid(res.result().sessionID());
	}

	public MultiContextConnect setSessionManager(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
		return this;
	}

	public MultiContextConnect setHost(String host) {
		this.host = host;
		return this;
	}
}
