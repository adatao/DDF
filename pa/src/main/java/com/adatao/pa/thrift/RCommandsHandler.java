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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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

import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.SparkConf;

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

	boolean isAdminUser(String clientID){
		return clientID.equals(SessionManager.ADMINUSER());
	}
	
	public JsonResult sendJsonCommand(final JsonCommand cmd) throws TException, JsonSyntaxException, ClassNotFoundException {
		if (!sessionManager.hasSession(cmd.getSid())) {
			return new JsonResult().setResult(new FailResult().setMessage("Session closed. Please reconnect!"));
		}

		final SparkThread sparkThread = (SparkThread) sessionManager.getSessionThread(cmd.getSid());
		String clientID = sessionManager.getClientID(cmd.getSid());
		String resultStr;
		
		try {
			if(Boolean.parseBoolean(System.getProperty("pa.authentication"))){
				if(isAdminUser(clientID) && !Boolean.parseBoolean(System.getProperty("run.as.admin"))){
					LOG.error("Admin user is prohibited to run command here. She can only run the first connect command");
					return new JsonResult().setResult(new FailResult().setMessage("This user is prohibited to run this command"));
				}
				
				Configuration conf = SparkHadoopUtil.get().newConfiguration();				
				UserGroupInformation.setConfiguration(conf);
				
				UserGroupInformation ugi;
				if (Boolean.parseBoolean(System.getProperty("run.as.admin"))){
					LOG.info("Execute command as admin");
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SessionManager.ADMINUSER(), 
							System.getProperty("pa.keytab.file"));
				} else {
					LOG.info("Execute command as: "+clientID);
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientID, 
							System.getProperty("pa.keytab.file"));
				}
				
//				UserGroupInformation ugi = 
//	                     UserGroupInformation.createProxyUser(clientID, adminUgi);
				
				resultStr = ugi.doAs(new PrivilegedExceptionAction<String>() {
					public String run() throws Exception {
						return sparkThread.processJsonCommand1(cmd).toJson();
					}
				});
			} else {
				resultStr = sparkThread.processJsonCommand1(cmd).toJson();
			}
			LOG.info("Returning result: " + resultStr);
			return new JsonResult().setResult(resultStr);
		} catch (Exception e) {
			LOG.error("Exception: ", e);
			Exception processedException = ExecutionResultUtils.processException(e);
			return new JsonResult().setResult(new FailResult().setMessage(processedException.getMessage()));			
		}
	}

	public JsonResult disconnect(JsonCommand cmd) throws TException, FileNotFoundException {
		LOG.info("Disconnect session " + cmd.getSid());
//		sessionManager.stopSession(cmd.getSid());
		return new JsonResult().setSid(cmd.getSid());
	}

	@Override
	public JsonResult execJsonCommand(JsonCommand cmd) throws TException {
		LOG.info("Execute command: " + cmd);
		if (cmd.getCmdName().toLowerCase().equals("connect")) {
			// this is a newer way to do connect
			try {
				Gson gson = new GsonBuilder()
					.excludeFieldsWithoutExposeAnnotation()
					.create();
				
				final Connect connect;
				if (cmd.params == null || cmd.params.equals("")) {
					connect = new Connect().setSessionManager(sessionManager);
				} else {
					connect = gson.fromJson(cmd.params, Connect.class).setSessionManager(sessionManager);
				}
				
				JsonResult res; 
						
				connectLock.lock();
				if(Boolean.parseBoolean(System.getProperty("pa.authentication")) == true){
					Configuration conf = SparkHadoopUtil.get().newConfiguration();				
					UserGroupInformation.setConfiguration(conf);
					
					UserGroupInformation ugi;
					if (Boolean.parseBoolean(System.getProperty("run.as.admin"))){
						LOG.info("Execute command as admin");
						ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SessionManager.ADMINUSER(), 
								System.getProperty("pa.keytab.file"));
					} else {
						String clientID = connect.getClientID();
						LOG.info("Execute command as: "+clientID);
						ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientID, 
								System.getProperty("pa.keytab.file"));
					}
					
					res = ugi.doAs(new PrivilegedExceptionAction<JsonResult>() {
						public JsonResult run() throws Exception {
							return connect.run();
						}
					});
				} else {
					res = connect.run();
				}
				LOG.info(res.toString());
				connectLock.unlock();
				return res;
			} catch (Exception e) {
				LOG.info("Exception: ", e);
				connectLock.unlock();
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
