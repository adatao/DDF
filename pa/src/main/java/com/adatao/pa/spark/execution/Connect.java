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
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.FailedResult;
import com.adatao.pa.thrift.Session;
import com.adatao.pa.thrift.SessionManager;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.annotations.Expose;

public class Connect {
  public static Logger LOG = LoggerFactory.getLogger(Connect.class);
  @Expose private String clientID;
  @Expose private Boolean isShark;

  private SessionManager sessionManager;


  public Connect() {
    this.isShark = true;
  }

  public Connect(String clientID, Boolean isShark) {
    this.clientID = clientID;
    this.isShark = isShark;
  }
  
  public String getClientID(){
	  return clientID;
  }
  
  /**
   * All clientIDs are currently shared a same session, at the same time we want to keep track of clientID that map to SparkThread
   * We do that by assign the single SparkThread to adminuser user when it is first created
   * In later connection, we will check if adminuser already have a SparkThread.
   * If yes, the user will be assigned that SparkThread.
   * 
   * 
   * @return JsonResult
   * @throws InterruptedException
   */

  public JsonResult run() throws InterruptedException {
    LOG.info("CLIENT ID: " + clientID);
    
    String adminuser = SessionManager.ADMINUSER();
    String adminSessionID = sessionManager.getSessionID(adminuser);
    if(clientID == null) {
      LOG.error("ClientID is null, cannot connect to PA");
      return new JsonResult().setResult(new FailResult().setMessage("You need to specify clientID"));
    }
    if (clientID.equals(adminuser) && adminSessionID!=null && !Boolean.parseBoolean(System.getProperty("run.as.admin"))){
    	LOG.error("Someone try to connect as an adminuser");
    	return new JsonResult().setResult(new FailResult().setMessage("You cannnot connect using admin user"));
    }
    
    // else if client is adminuser and this is the first connection by adminuser, then do it
    else if (clientID.equals(adminuser) && adminSessionID==null){
    	SparkThread sparkThread = (SparkThread) new SparkThread().setShark(isShark);
    	
    	if (sparkThread.startSession()){
        	//uiPort and driverPort information is not important when we run single context
    		Session session = sessionManager.addSession(sparkThread, adminuser, 0, 0, 0);
    		if (session != null) {
    			String sessionID = session.sessionID();
    			return new JsonResult().setSid(sessionID);
    		} else {
    			return new JsonResult().setResult(new FailedResult<Object>(AdataoExceptionCode.ERR_GENERAL.getMessage()).toJson());
    		}
        } else {    	
        	return new JsonResult().setResult(new FailedResult<Object>(AdataoExceptionCode.ERR_GENERAL.getMessage()).toJson());
        }
    }
    
    // clientID cannot be admin user now
    
    // if user already connected
    else if (sessionManager.hasClient(clientID)) {
    	return new JsonResult().setSid(sessionManager.getSessionID(clientID));
    }
    // else if client has not connected and adminSessionID exists, then give that session to the client
    else if (adminSessionID != null) {
    	SparkThread sparkThread = sessionManager.getSessionThread(adminSessionID);
    	Session session = sessionManager.addSession(sparkThread, clientID, 0, 0, 0);
		if (session != null) {
			String sessionID = session.sessionID();
			return new JsonResult().setSid(sessionID);
		} else {
			return new JsonResult().setResult(new FailedResult<Object>(AdataoExceptionCode.ERR_GENERAL.getMessage()).toJson());
		}
    } else {
    	LOG.error("The adminuser session does not exist");
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
