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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.RCommands;
import com.adatao.pa.thrift.generated.RCommands.Client;

import org.apache.spark.deploy.SparkHadoopUtil;

public class Server {
	int port;
	static String host = "localhost";
	TServer server;
	RCommands.Iface handler;
	public static Boolean MULTIUSER_DEFAULT = false;
	static Boolean multiUser = Boolean.parseBoolean(System.getProperty("bigr.multiuser", MULTIUSER_DEFAULT.toString()));

	public Logger LOG = LoggerFactory.getLogger(Server.class);

	private static SessionManager sessionManager;

	public SessionManager getSessionManager() {
		return sessionManager;
	}

	// lock for the server control plane
	private ReentrantLock serverLock = new ReentrantLock();

	public Server(int port) {
		this.port = port;
		this.sessionManager = new SessionManager(port);
	}

	public void start() {
		class ServerThread extends Thread {
			public void run(){
				try {
					// Set port
					serverLock.lock();
					TServerSocket serverTransport = new TServerSocket(port);
					handler = new RCommandsHandler(sessionManager);
					RCommands.Processor<RCommands.Iface> processor = new RCommands.Processor<RCommands.Iface>(handler);
	
					server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
					serverLock.unlock();
					
					LOG.info("Starting thrift server on port " + port);
					server.serve();
				} catch (TTransportException e) {
					e.printStackTrace();
					LOG.error("Cannot start server", e);
				}
			}
		}
		new ServerThread().start();
	}

	public void stop() {
		LOG.info("Stopping thrift server on port " + port + "...");
		serverLock.lock();
		sessionManager.stopAllSession();
		server.stop();
		serverLock.unlock();
		LOG.info("Stopped thrift server on port " + port);
	}
	
	/**
	 * Make the first connection to start workers on slave nodes   
	 */
	public static void makeFirstConnection(String host, int port) throws TTransportException, TException{
		TTransport socket = new TSocket(host, port);
		socket.open();
		TBinaryProtocol protocol = new TBinaryProtocol(socket);
		Client client = new RCommands.Client(protocol);
		//connect first as ADMINUSER
		client.execJsonCommand(new JsonCommand().setCmdName("connect").setParams(String.format("{clientID: %s}", SessionManager.ADMINUSER())));
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, TTransportException, TException {
		int port = Integer.parseInt(System.getProperty("bigr.server.thriftPort", "7911"));
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		final Server server = new Server(port);
		if(Boolean.parseBoolean(System.getProperty("pa.authentication")) == true){
			Configuration conf = SparkHadoopUtil.get().newConfiguration();
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(System.getProperty("pa.admin.user"),
					System.getProperty("pa.keytab.file"));

System.out.println(">>>>>>>> +"+ugi.getRealAuthenticationMethod());
			class ServerPrivilegedAction implements PrivilegedExceptionAction<Void> {
				String host;
				int port;

				public ServerPrivilegedAction(String host, int port){
					this.host = host;
					this.port = port;
				}

				@Override
				public Void run() throws Exception {
					server.start();
					makeFirstConnection(host, port);
					return (null);
				}

			}
			ugi.doAs(new ServerPrivilegedAction(host, port));
      server.start();
      makeFirstConnection(host, port);
			
		} else {
			server.start();
			makeFirstConnection(host, port);
		}
		
		
	}
}
