package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult
import java.util.ArrayList
import java.util.Arrays
import com.adatao.pa.spark.execution.QuickSummary
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.ddf.DDF.JoinType
import com.adatao.pa.spark.DataManager.MetaInfo

class JoinSuite extends ABigRClientTest {

  override def beforeAll = {
    super.beforeAll
    createTableMtcars
    createTableCarowner
  }

  test("test inner join") {
    val leftdf = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(leftdf.isSuccess)
    val leftdcID = leftdf.dataContainerID
    LOG.info("Got dataContainerID = " + leftdcID)
    val rightdf = this.runSQL2RDDCmd("SELECT * FROM carowner", true)
    assert(rightdf.isSuccess)
    val rightdcID = rightdf.dataContainerID
    LOG.info("Got dataContainerID = " + rightdcID)
    var cmd = new Join();
    cmd.setLeftDataContainerID(leftdcID);
    cmd.setRightDataContainerID(rightdcID);
    cmd.setByColumns(Arrays.asList("cyl"));
    cmd.setJoinType(JoinType.INNER);
    val result = bigRClient.execute[DataFrameResult](cmd).result
    println(">>>>result=" + result)
    var cmd2 = new QuickSummary();
    cmd2.setDataContainerID(result.dataContainerID)
    val result2 = bigRClient.execute[DataframeStatsResult](cmd2).result
    println(">>>>result2=" + result2)

    val metaInfo: Array[MetaInfo] = result.metaInfo;
    assert(metaInfo(11).getHeader() === "r_name");
    assert(metaInfo(12).getHeader() === "r_disp");

    assert(result2.count(0) === 25);
  }

  test("test left join") {
    val leftdf = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(leftdf.isSuccess)
    val leftdcID = leftdf.dataContainerID
    LOG.info("Got dataContainerID = " + leftdcID)
    val rightdf = this.runSQL2RDDCmd("SELECT * FROM carowner", true)
    assert(rightdf.isSuccess)
    val rightdcID = rightdf.dataContainerID
    LOG.info("Got dataContainerID = " + rightdcID)
    var cmd = new Join();
    cmd.setLeftDataContainerID(leftdcID);
    cmd.setRightDataContainerID(rightdcID);
    cmd.setByColumns(Arrays.asList("cyl"));
    cmd.setJoinType(JoinType.LEFT);
    val result = bigRClient.execute[DataFrameResult](cmd).result
    println(">>>>result=" + result)

    val nrow = new NRow().setDataContainerID(result.dataContainerID)
    val result2 = bigRClient.execute[DataframeStatsResult](nrow).result
    println(">>>>result2=" + result2)

    assert(result2.count(0) === 39);
  }

  test("test right join") {
    val leftdf = this.runSQL2RDDCmd("SELECT * FROM mtcars", true)
    assert(leftdf.isSuccess)
    val leftdcID = leftdf.dataContainerID
    LOG.info("Got dataContainerID = " + leftdcID)
    val rightdf = this.runSQL2RDDCmd("SELECT * FROM carowner", true)
    assert(rightdf.isSuccess)
    val rightdcID = rightdf.dataContainerID
    LOG.info("Got dataContainerID = " + rightdcID)
    var cmd = new Join();
    cmd.setLeftDataContainerID(leftdcID);
    cmd.setRightDataContainerID(rightdcID);
    cmd.setByColumns(Arrays.asList("cyl"));
    cmd.setJoinType(JoinType.RIGHT);
    val result = bigRClient.execute[DataFrameResult](cmd).result
    println(">>>>result=" + result)

    val nrow = new NRow().setDataContainerID(result.dataContainerID)
    val result2 = bigRClient.execute[DataframeStatsResult](nrow).result
    println(">>>>result2=" + result2)

    assert(result2.count(0) === 26);

  }

}