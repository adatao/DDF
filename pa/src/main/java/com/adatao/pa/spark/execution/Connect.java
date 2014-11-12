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

import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.FailedResult;
import com.adatao.pa.thrift.SessionManager;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.annotations.Expose;

public class Connect {
  public static Logger LOG = LoggerFactory.getLogger(Connect.class);
  @Expose private String clientID;
  @Expose private Boolean isShark;

  private SessionManager sessionManager;


  public Connect() {
    this.clientID = SessionManager.ANONYMOUS();
    this.isShark = true;
  }

  public Connect(String clientID, Boolean isShark) {
    this.clientID = clientID;
    this.isShark = isShark;
  }

  /**
   * All clientIDs are currently shared a same session, at the same time we want to keep track of clientID that map to SparkThread
   * We do that by assign the single SparkThread to anonymous user when it is first created
   * In later connection, we will check if anonymous already have a SparkThread.
   * If yes, the user will be assigned that SparkThread.
   * 
   * 
   * @return JsonResult
   * @throws InterruptedException
   */

  public JsonResult run() throws InterruptedException {
	// 
    LOG.info("CLIENT ID: " + clientID);
    
    // currently we map all user to anonymous 
    String anonymous = SessionManager.ANONYMOUS();
    
    // if user already connected
    if (sessionManager.hasClient(clientID)) {
    	return new JsonResult().setSid(sessionManager.getSessionID(clientID));
    }
    
    // if someone else already connected (which means anonymous user has a SparkThread)
    if (sessionManager.hasClient(anonymous)) {
    	//get sessionID from anonymous and assign it to clientID
    	String sessionID = sessionManager.getSessionID(anonymous);
    	SparkThread sparkThread = sessionManager.getSessionThread(sessionID);
    	sessionManager.addSession(sparkThread, clientID, 0, 0, 0);  
    	return new JsonResult().setSid(sessionID);
    }

    // if this is the first connect ever
    SparkThread sparkThread = (SparkThread) new SparkThread().setShark(isShark);
    
    if (sparkThread.startSession()){
    	//uiPort and driverPort information is not important when we run single context
    	String sessionID = sessionManager.addSession(sparkThread, clientID, 0, 0, 0).sessionID();
    	
    	//assign the sparkThead to anynymous too
    	sessionManager.addSession(sparkThread, anonymous, 0, 0, 0);
    	return new JsonResult().setSid(sessionID);
    } else {    	
    	return new JsonResult().setResult(new FailedResult<Object>(AdataoExceptionCode.ERR_GENERAL.getMessage()).toJson());
    }

    
  }

  public Connect setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
    return this;
  }

  // public String getClientID() {
  // return clientID;
  // }

}
