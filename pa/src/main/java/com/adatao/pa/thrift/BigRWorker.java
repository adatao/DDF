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
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.thrift.generated.RCommands;

public class BigRWorker {
	int thriftPort;
	int uiPort;
	int driverPort;
	String host = "localhost";
	TServer worker;
	RWorkerCommandsHandler handler;
	static public Logger LOG = LoggerFactory.getLogger(BigRWorker.class);

	final String clientID;
	final String sid;
	
	/**
	 * There will be only one session in sessionManager
	 */
	SessionManager sessionManager;
		
	public SessionManager getSessionManager() {
		return sessionManager;
	}
	
	// lock for the server control plane
	private ReentrantLock workerLock =  new ReentrantLock();


	public BigRWorker(int thriftPort, int uiPort, int driverPort, String clientID, String sid){
		this.thriftPort = thriftPort;
		this.uiPort = uiPort;
		this.driverPort = driverPort;
		this.clientID = clientID;
		this.sid = sid;
		sessionManager = new SessionManager(thriftPort);
	}
	
	public void start() throws FileNotFoundException, TException, InterruptedException {		
		workerLock.lock();
		TServerSocket serverTransport = new TServerSocket(thriftPort);
		handler = new RWorkerCommandsHandler(uiPort, driverPort, clientID, sid, sessionManager);
		RCommands.Processor<RCommands.Iface> processor
			= new RCommands.Processor<RCommands.Iface>(handler);

		worker = new TThreadPoolServer(
			new TThreadPoolServer.Args(serverTransport).processor(processor));
		workerLock.unlock();

		LOG.info("Starting thrift worker on port "+thriftPort);
		worker.serve();
	}

	public void stop(){
		LOG.info("Stopping thrift worker on port "+thriftPort+"...");
		workerLock.lock();
		worker.stop();
		sessionManager.stopAllSession();
		workerLock.unlock();
		LOG.info("Stopped thrift worker on port "+thriftPort);
	}
	
	//TODO
	static void usage(){
		
	}
	
	/**
	 * If the Server is started asWorker, it will start a single SparkContext 
	 * @throws TException 
	 * @throws FileNotFoundException 
	 * @throws InterruptedException 
	 * 
	 */
	public static void main(String args[]) throws FileNotFoundException, TException, InterruptedException{
		int thriftPort;
		int uiPort;
		int driverPort;
		String clientID;
		String sid;
		
		if (args.length == 5){
			thriftPort = Integer.parseInt(args[0]);
			uiPort = Integer.parseInt(args[1]);
			driverPort = Integer.parseInt(args[2]);
			clientID = args[3];
			sid = args[4];
			
			BigRWorker worker = new BigRWorker(thriftPort, uiPort, driverPort, clientID, sid);
			worker.start();
		}
		else{
			LOG.info("Wrong number of args");
			usage();
			System.exit(1);
		}
	}
}
