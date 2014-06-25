package com.adatao.pa.spark.execution;


import java.util.Arrays;
import junit.framework.Assert;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.execution.Aggregate.AggregateResult;
import com.adatao.pa.spark.execution.NRow.NRowResult;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;

public class TestMissingDataHandling extends BaseTest {
  public static Logger LOG = LoggerFactory.getLogger(TestGroupBy.class);

  String airlineID;
  String sid;


  @Before
  public void init() throws TException {
    JsonCommand cmd = new JsonCommand().setCmdName("connect");
    Gson gson = new Gson();

    JsonResult res = client.execJsonCommand(cmd);
    sid = res.sid;
    LOG.info("Got session ID: " + sid);
    assert (sid != null);

    createTableAirlineWithNA(sid);

    Sql2DataFrame.Sql2DataFrameResult ddf = this.runSQL2RDDCmd(sid, "SELECT * FROM airline", true);
    airlineID = ddf.dataContainerID;
  }

  @Test
  public void testDropNA() throws TException {
    JsonCommand cmd = new JsonCommand();
    JsonResult res;
    com.adatao.pa.spark.Utils.DataFrameResult dropNAResult;
    NRowResult nrResult;
    MetaInfo[] metaInfo;

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("DropNA")
        .setParams(
            String.format("{dataContainerID: %s," + "axis: row," + "how: any," + "thresh: 0," + "columns: null}",
                airlineID)));
    dropNAResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = dropNAResult.getMetaInfo();
    LOG.info("DropNA result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("year"));
    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("NRow")
        .setParams(String.format("{dataContainerID: %s}", dropNAResult.getDataContainerID())));
    nrResult = ExecutionResult.fromJson(res.getResult(), NRowResult.class).result();
    Assert.assertEquals(nrResult.nrow, 9);
  }

  @Test
  public void testFillNA() throws TException {
    JsonCommand cmd = new JsonCommand();
    JsonResult res;
    com.adatao.pa.spark.Utils.DataFrameResult fillNAResult;
    NRowResult nrResult;
    AggregateResult aggResult;
    MetaInfo[] metaInfo;

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("FillNA")
        .setParams(
            String.format("{dataContainerID: %s," + "value: 1," + "method: null," + "limit: 0," + "function: null,"
                + "columnsToValues: null," + "columns: null}", airlineID)));
    fillNAResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = fillNAResult.getMetaInfo();
    LOG.info("FillNA result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("year"));
    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("NRow")
        .setParams(String.format("{dataContainerID: %s}", fillNAResult.getDataContainerID())));
    nrResult = ExecutionResult.fromJson(res.getResult(), NRowResult.class).result();
    Assert.assertEquals(nrResult.nrow, 31);

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("Aggregate")
        .setParams(
            String.format("{dataContainerID: %s," + "colnames: [LateAircraftDelay]," + "groupBy: [year],"
                + "func: sum}", fillNAResult.getDataContainerID())));
    aggResult = ExecutionResult.fromJson(res.getResult(), AggregateResult.class).result();
    Assert.assertEquals(aggResult.results.get("2008")[0], 301, 0.1);
  }

}
