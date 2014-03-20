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
import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.thrift.Server;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;
import com.google.gson.Gson;

public class TestLoadTable {
	Server tServer;
	String host = "localhost";
	int port = 7912;
	TTransport transport;
	RCommands.Client client;
	
	public static Logger LOG = LoggerFactory.getLogger(TestLoadTable.class);	
	
	@Test
	public void testGetType() {
		String[] testStr = {"123", "456"};
		String type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.Integer");
		
		testStr = new String[] {"123,456.123", "123", "123,456", "456.123", ".123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.Double");
		
		testStr = new String[] {"123,456.123", "123", "123,456", "456.123", "a.123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"123,456.123", "123", "123,456", "456.123", "..123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"123,456.123", "123", "123,456", "456.123", ",123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"123,456.123", "123", "123,456", "456."};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"123,456.abc", "123", "123,456", "456.123", "..123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"123,456", "NA", "123,456", "456.123", ".123"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.Double");
		
		testStr = new String[] {"123,456", "T", "F", "F", "T"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.String");
		
		testStr = new String[] {"NA", "T", "F", "F", "T"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="java.lang.Boolean");
		
		testStr = new String[] {"NA", "NA", "NA", "NA", "NA"};
		type = LoadTable.determineType(testStr, false);
		LOG.info("Type: "+type);
		assert(type=="Unknown");
	}


	@Test
	public void testBuildDataFrameWithoutHeader() throws IOException {
		JavaSparkContext sc = new JavaSparkContext(System.getenv("SPARK_MASTER"), "BigR",
				System.getenv("SPARK_HOME"), System.getenv("RSERVER_JAR").split(","));

		try {
			LoadTable lt = new LoadTable();
			String fileURL = "resources/table_noheader.csv";
			JavaRDD<String> fileRDD = sc.textFile(fileURL);

			MetaInfo[] metaInfoArray = lt.getMetaInfo(fileRDD);
			LOG.info(Arrays.toString(metaInfoArray));
			Broadcast<MetaInfo[]> broadcastMetaInfo = sc.broadcast(metaInfoArray);

			JavaRDD<Object[]> dataFrame = lt.getDataFrame(fileRDD, broadcastMetaInfo);

			List<Object[]> dataList = dataFrame.collect();
			for (Object[] data : dataList) {
				LOG.info("DataFrame row: " + Arrays.toString(data));
			}
		} finally {
			sc.stop();
		}
	}
	
//	@Test
//	public void testBuildDataFrameWithHeader() throws IllegalArgumentException, 
//	IllegalAccessException, InvocationTargetException, SecurityException, 
//	NoSuchMethodException, ClassNotFoundException, NoSuchFieldException {
//		assert(System.getenv("SPARK_MASTER")!=null);
//		assert(System.getenv("SPARK_HOME")!=null);
//		
//		final String SPARK_MASTER = System.getenv("SPARK_MASTER");
//		final String SPARK_HOME = System.getenv("SPARK_HOME");
//		final String[] JOB_JAR = {	            
//	            System.getenv("RSERVER_HOME")+"/target/rserver-0.1.0.jar"};
//		
//		JavaSparkContext sc = new JavaSparkContext(SPARK_MASTER, "BigR",
//				SPARK_HOME, JOB_JAR);
//		
//		LoadTable lt = new LoadTable();
//		String fileURL = "resources/table.csv";
//		JavaRDD<String> fileRDD = sc.textFile(fileURL);
//		
//		Method getMetaInfo = Class.forName("com.adatao.pa.spark.execution.LoadTable")
//				.getDeclaredMethod("getMetaInfo", JavaRDD.class);
//		getMetaInfo.setAccessible(true);
//		MetaInfo[] metaInfoArray = 
//				(MetaInfo[]) getMetaInfo.invoke(
//						lt.setFileURL(fileURL)
//							.setHasHeader(true).setSeparator(","), 
//						fileRDD);
//		LOG.info(Arrays.toString(metaInfoArray));		
//		
//		Field bcMetaInfo = Class.forName("com.adatao.pa.spark.execution.LoadTable")
//				.getDeclaredField("broadcastMetaInfo");
//		bcMetaInfo.setAccessible(true); 
//		bcMetaInfo.set(lt, sc.broadcast(metaInfoArray));
//		
//		Method getDataFrame = Class.forName("com.adatao.pa.spark.execution.LoadTable")
//				.getDeclaredMethod("getDataFrame", JavaRDD.class);
//		getDataFrame.setAccessible(true);
//		JavaRDD<Object[]> dataFrame = 
//				(JavaRDD<Object[]>) getDataFrame.invoke(lt, fileRDD);
//		
//		try {
//			List<Object[]> dataList = dataFrame.collect();
//			for (Object[] data: dataList){
//				LOG.info("DataFrame row: "+Arrays.toString(data));
//			}
//		} 
//		catch (Exception e){
//			LOG.info(e.toString());
//			fail();
//		}
//		sc.stop();
//	}
	
	public void startServer() throws Exception {
		tServer = new Server(port);
		
		new Thread(new Runnable() {
			public void run(){				
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
            stopServer();
            fail();
        }
	}

	
	public void stopServer() throws Exception {
		tServer.stop();
		transport.close();
		Thread.sleep(5000);
	}

	@Test
	public void testJsonCommand() throws Exception {
		System.out.println("TEST JSON.............");
		startServer();

        try {
        	JsonCommand cmd = new JsonCommand().setCmdName("connect");
            Gson gson = new Gson();

            JsonResult res = client.execJsonCommand(cmd);
            String sid = res.sid;
            LOG.info("Got session ID: " + sid);
            Thread.sleep(5000);
            LoadTable loadTbl = (LoadTable) new LoadTable()
                    .setFileURL("resources/table.csv")
                    .setHasHeader(true)
                    .setSeparator(",");

            LOG.info(gson.toJson(loadTbl));
            cmd.setSid(sid)
                    .setCmdName("LoadTable")
                    .setParams(gson.toJson(loadTbl));

            res = client.execJsonCommand(cmd);

            cmd.setCmdName("disconnect")
                    .setSid(sid)
                    .setParams(null);
            res = client.execJsonCommand(cmd);
            String newSid = res.sid;
            assert (newSid.equals(sid));
        } finally {
        	stopServer();
        }
	}	
	
}
