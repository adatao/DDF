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
import com.adatao.pa.spark.execution.NRow.NRowResult;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;

public class TestGroupBy extends BaseTest {

  public static Logger LOG = LoggerFactory.getLogger(TestGroupBy.class);

  String mtcarsID;
  String sid;


  @Before
  public void init() throws TException {
    JsonCommand cmd = new JsonCommand().setCmdName("connect");
    Gson gson = new Gson();

    JsonResult res = client.execJsonCommand(cmd);
    sid = res.sid;
    LOG.info("Got session ID: " + sid);
    assert (sid != null);

    createTableMtcars(sid);

    Sql2DataFrame.Sql2DataFrameResult ddf = this.runSQL2RDDCmd(sid, "SELECT * FROM mtcars", true);
    mtcarsID = ddf.dataContainerID;

  }

  @Test
  public void testGroupBy() throws TException {
    JsonCommand cmd = new JsonCommand();
    JsonResult res;
    com.adatao.pa.spark.Utils.DataFrameResult groupbyResult;
    NRowResult nrResult;
    MetaInfo[] metaInfo;

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl]," + "selectFunctions: [mean(disp)]}",
                mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = groupbyResult.getMetaInfo();
    LOG.info("GroupBy result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("c_0"));

    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("NRow")
        .setParams(String.format("{dataContainerID: %s}", groupbyResult.getDataContainerID())));
    nrResult = ExecutionResult.fromJson(res.getResult(), NRowResult.class).result();
    Assert.assertEquals(nrResult.nrow, 3);

    // count(disp), count(*) and count(1) work
    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl],"
                + "selectFunctions: ['metrics= count(*)']}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = groupbyResult.getMetaInfo();
    LOG.info("GroupBy result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("metrics"));

    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("NRow")
        .setParams(String.format("{dataContainerID: %s}", groupbyResult.getDataContainerID())));
    nrResult = ExecutionResult.fromJson(res.getResult(), NRowResult.class).result();
    Assert.assertEquals(nrResult.nrow, 3);

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String
                .format("{dataContainerID: %s," + "groupedColumns: [cyl]," + "selectFunctions: ['adisp=mean(disp)']}",
                    mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = groupbyResult.getMetaInfo();
    LOG.info("GroupBy result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("adisp"));
    Assert.assertTrue(metaInfo[1].getHeader().equals("cyl"));


    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl, disp],"
                + "selectFunctions: ['smpg=stddev(disp)']}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();

    metaInfo = groupbyResult.getMetaInfo();
    LOG.info("GroupBy result: " + Arrays.toString(metaInfo));

    Assert.assertTrue(metaInfo[0].getHeader().equals("smpg"));
    Assert.assertTrue(metaInfo[1].getHeader().equals("cyl"));
    Assert.assertTrue(metaInfo[2].getHeader().equals("disp"));

    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("NRow")
        .setParams(String.format("{dataContainerID: %s}", groupbyResult.getDataContainerID())));
    nrResult = ExecutionResult.fromJson(res.getResult(), NRowResult.class).result();
    Assert.assertEquals(nrResult.nrow, 27);


  }

  @Test
  public void testNullResult() throws TException {
    JsonCommand cmd = new JsonCommand();
    JsonResult res;
    com.adatao.pa.spark.Utils.DataFrameResult groupbyResult;
    NRowResult nrResult;
    MetaInfo[] metaInfo;

    // Negative tests
    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [],"
                + "selectFunctions: ['smpg=stddev(disp)', disp, cyl]}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    res = client
        .execJsonCommand(cmd
            .setSid(sid)
            .setCmdName("GroupBy")
            .setParams(
                String.format("{dataContainerID: %s," + "groupedColumns: [cyl]," + "selectFunctions: []}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    // Missing dataframe
    res = client.execJsonCommand(cmd.setSid(sid).setCmdName("GroupBy")
        .setParams(String.format("{" + "groupedColumns: [cyl]," + "selectFunctions: [mean(disp)]}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    // Unsupported function
    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl]," + "selectFunctions: [blah(disp)]}",
                mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl1]," + "selectFunctions: [mean(disp)]}",
                mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);
  }

  @Test
  public void testWrongSelectFucntion() throws TException {
    JsonCommand cmd = new JsonCommand();
    JsonResult res;
    com.adatao.pa.spark.Utils.DataFrameResult groupbyResult;
    NRowResult nrResult;
    MetaInfo[] metaInfo;

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl, mpg],"
                + "selectFunctions: [avg(disp), '=mean(dfsdf']}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl, mpg]," + "selectFunctions: [avg(disp), *]}",
                mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);

    res = client.execJsonCommand(cmd
        .setSid(sid)
        .setCmdName("GroupBy")
        .setParams(
            String.format("{dataContainerID: %s," + "groupedColumns: [cyl, mpg],"
                + "selectFunctions: [avg(disp), '=abc']}", mtcarsID)));
    groupbyResult = ExecutionResult.fromJson(res.getResult(), com.adatao.pa.spark.Utils.DataFrameResult.class).result();
    Assert.assertNull(groupbyResult);
  }
}
