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

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.thrift.SessionManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.annotations.Expose;

public class Connect {
	public static Logger LOG = LoggerFactory.getLogger(Connect.class);
	@Expose private String clientID;
	@Expose private Boolean isShark;

	private SessionManager sessionManager;

	public Connect(){
		this.clientID = SessionManager.ANONYMOUS();
		this.isShark = true;
	}
	
	public Connect(String clientID, Boolean isShark){
		this.clientID = clientID;
		this.isShark = isShark;
	}
	
	// sparkThread is not used in this case because there is no sparkThread yet
	public JsonResult run() throws InterruptedException {
		LOG.info("CLIENT ID: " + clientID);
		clientID=SessionManager.ANONYMOUS();
		if (sessionManager.hasClient(clientID))
			return new JsonResult().setSid(sessionManager.getSessionID(clientID));

		
		ArrayBlockingQueue<Object> cmdQueue = new ArrayBlockingQueue<Object>(1);
		ArrayBlockingQueue<Object> resQueue = new ArrayBlockingQueue<Object>(1);

		SparkThread sparkThread = (SparkThread) new SparkThread(cmdQueue, resQueue).setClientID(clientID)
				.setSessionManager(sessionManager).setShark(isShark);		
		sparkThread.run();
		//Thread will put a result into queue after restart, we have to get it out
		return (JsonResult) resQueue.take();
	}

	public Connect setSessionManager(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
		return this;
	}

	// public String getClientID() {
	// return clientID;
	// }

}
