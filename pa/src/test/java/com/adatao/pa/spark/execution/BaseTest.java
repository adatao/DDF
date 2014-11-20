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

import com.adatao.pa.spark.types.BigRThriftServerUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.thrift.Server;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.adatao.pa.thrift.generated.RCommands;
import com.google.gson.Gson;

public class BaseTest {
  static Server tServer;
  static String host = "localhost";
  static int port = 7912;
  static TTransport transport;
  public static RCommands.Client client;


  @BeforeClass
  public static void startServer() throws Exception {
    tServer = new Server(port);

    new Thread(new Runnable() {
      public void run() {
        tServer.start();
      }
    }).start();

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

  @AfterClass
  public static void stopServer() throws Exception {
    tServer.stop();
    transport.close();
  }

  @Before
  public void rest4AWhile() throws Exception {
    Thread.sleep(500);
  }


  Sql2ListString.Sql2ListStringResult runSQLCmd(String sid, String cmdStr) throws TException {
    Sql2ListString sql = new Sql2ListString().setSqlCmd(cmdStr);
    JsonCommand cmd = new JsonCommand();
    Gson gson = new Gson();
    JsonResult res;
    Sql2ListString.Sql2ListStringResult sResult;

    cmd.setCmdName("Sql2ListString").setSid(sid).setParams(gson.toJson(sql));
    res = client.execJsonCommand(cmd);
    sResult = ExecutionResult.fromJson(res.getResult(), Sql2ListString.Sql2ListStringResult.class).result();
    System.out.println(">>>>runSQLCmd" + cmdStr);
    return sResult;
  }

  public Sql2DataFrame.Sql2DataFrameResult runSQL2RDDCmd(String sid, String cmdStr, Boolean cache) throws TException {
    Sql2DataFrame sql = new Sql2DataFrame(cmdStr, cache);
    JsonCommand cmd = new JsonCommand();
    Gson gson = new Gson();
    JsonResult res;

    cmd.setCmdName("Sql2DataFrame").setSid(sid).setParams(gson.toJson(sql));
    res = client.execJsonCommand(cmd);
    return ExecutionResult.fromJson(res.getResult(), Sql2DataFrame.Sql2DataFrameResult.class).result();
  }


  void createTableMtcars(String sid) throws TException {
    runSQLCmd(sid, "drop table if exists mtcars");
    runSQLCmd(
        sid,
        "CREATE TABLE mtcars ("
            + "mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb int"
            + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '");
    runSQLCmd(sid, "LOAD DATA LOCAL INPATH 'resources/mtcars' INTO TABLE mtcars");
  }


  void createTableAirlineWithNA(String sid) throws TException {
    runSQLCmd(sid, "drop table if exists airline");
    runSQLCmd(sid, "create table airline (year int,month int,dayofmonth int,"
        + "dayofweek int,deptime int,crsdeptime int,arrtime int,"
        + "crsarrtime int,uniquecarrier string, flightnum int, "
        + "tailnum string, actualelapsedtime int, crselapsedtime int, "
        + "airtime int, arrdelay int, depdelay int, origin string, "
        + "dest string, distance int, taxiin int, taxiout int, cancelled int, "
        + "cancellationcode string, diverted string, carrierdelay int, "
        + "weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    runSQLCmd(sid, "LOAD DATA LOCAL INPATH 'resources/airlineWithNa.csv' INTO TABLE airline");
  }
}
