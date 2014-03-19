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

import static org.junit.Assert.fail;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.execution.Sql2ListString;
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult;
import com.adatao.pa.thrift.Server;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;
import com.google.gson.Gson;

/**
 * This is similar to BaseTest except that it startServer and stopServer are not
 * annotated with BeforeClass and AfterClass. This is useful for tests that do
 * not require to start the server at every tests.
 * <p>
 * There is also function to start server with different serverHost and port
 * 
 * @author bachbui
 * 
 */
@Deprecated // [ctn] this is too dangerous if not used properly; if stopServer() isn't called due to some exception thrown, the test suites are stuck
public class BaseTest2 {
	Server tServer;
	int port = 7912;
	TTransport transport;
	public RCommands.Client client;
	public static Logger LOG = LoggerFactory.getLogger(BaseTest2.class);

	/**
	 * The server will only be start in localhost
	 */
	public void startServer(int port) throws Exception {
		this.port = port;
		startServer();
	}

	/**
	 * The client created after starting server will connect to the server on
	 * localhost. If you want to have a client connect to different host please
	 * use createClient function
	 */
	public void startServer() throws Exception {
		if (tServer != null) {
			LOG.info("Another server is running, will have to stop it first");
			stopServer();
		}

		tServer = new Server(port);

		new Thread(new Runnable() {
			public void run() {
				tServer.start();
			}
		}).start();

		Thread.sleep(5000);

		try {
			transport = new TSocket("localhost", port);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			client = new RCommands.Client(protocol);
		} catch (TTransportException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void stopServer() throws Exception {
		tServer.stop();
		tServer = null;
		stopClient();
	}

	public void createClient(String host) {
		if (host == null) {
			host = "localhost";
		}
		if (client !=null){
			LOG.info("Another client has been opened. Will close that one first");
			stopClient();
		}
		try {
			transport = new TSocket(host, port);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			client = new RCommands.Client(protocol);
		} catch (TTransportException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void stopClient(){
		transport.close();
		client = null;
	}
	
	Sql2ListString.Sql2ListStringResult runSQLCmd(String sid, String cmdStr) throws TException {
		Sql2ListString sql = new Sql2ListString().setSqlCmd(cmdStr);
		JsonCommand cmd = new JsonCommand();
		Gson gson = new Gson();
		JsonResult res;
		Sql2ListString.Sql2ListStringResult sResult;

		cmd.setCmdName("Sql2ListString").setSid(sid).setParams(gson.toJson(sql));
		res = client.execJsonCommand(cmd);
		sResult = gson.fromJson(res.getResult(), Sql2ListString.Sql2ListStringResult.class);
		return sResult;
	}

	Sql2DataFrameResult runSQL2RDDCmd(String sid, String cmdStr, Boolean cache) throws TException {
		Sql2ListString sql = new Sql2ListString().setSqlCmd(cmdStr);
		JsonCommand cmd = new JsonCommand();
		Gson gson = new Gson();
		JsonResult res;

		cmd.setCmdName("Sql2DataFrame").setSid(sid).setParams(gson.toJson(sql));
		res = client.execJsonCommand(cmd);
		return gson.fromJson(res.getResult(), Sql2DataFrameResult.class);
	}
}
