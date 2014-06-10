package com.adatao.pa.spark.execution;


import java.util.List;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shark.api.JavaSharkContext;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDF.JoinType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.google.gson.annotations.SerializedName;

/**
 * Implement an join interface similar like what in plyr:
 * http://hosho.ees.hokudai.ac.jp/~kubo/Rdoc/library/plyr/html/join.html. and
 * http://stat.ethz.ch/R-manual/R-devel/library/base/html/merge.html Note that Shark/Hive don't support join with
 * matchType="first", so we don't include it in this interface. Also, the join type supported by Shark/Hive are
 * {LEFT|RIGHT|FULL} OUTER and LEFT SEMI
 * 
 * @author bachbui
 * 
 */
public class Join extends CExecutor {

  public static Logger LOG = LoggerFactory.getLogger(Join.class);

  private String leftDataContainerID;
  private String rightDataContainerID;
  private List<String> byColumns;
  private List<String> byLeftColumns;
  private List<String> byRightColumns;


  public String getLeftDataContainerID() {
    return leftDataContainerID;
  }

  public void setLeftDataContainerID(String leftDataContainerID) {
    this.leftDataContainerID = leftDataContainerID;
  }

  public String getRightDataContainerID() {
    return rightDataContainerID;
  }

  public void setRightDataContainerID(String rightDataContainerID) {
    this.rightDataContainerID = rightDataContainerID;
  }

  public List<String> getByColumns() {
    return byColumns;
  }

  public void setByColumns(List<String> byColumns) {
    this.byColumns = byColumns;
  }

  public List<String> getByLeftColumns() {
    return byLeftColumns;
  }

  public void setByLeftColumns(List<String> byLeftColumns) {
    this.byLeftColumns = byLeftColumns;
  }

  public List<String> getByRightColumns() {
    return byRightColumns;
  }

  public void setByRightColumns(List<String> byRightColumns) {
    this.byRightColumns = byRightColumns;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  private JoinType joinType = JoinType.INNER;
  
  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
//    Utils.assertNull(joinType, new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_JOIN_SYNTAX,
//          String.format("Unsupported join type"), null));
//    Utils.assertNull(leftDataContainerID, new AdataoException(AdataoExceptionCode.ERR_MISSING_DATAFRAME, null));
//    Utils.assertNull(rightDataContainerID, new AdataoException(AdataoExceptionCode.ERR_MISSING_DATAFRAME, null));
    
    String leftDdfId = com.adatao.ML.Utils.dcID2DDFID(leftDataContainerID);
    String rightDdfId = com.adatao.ML.Utils.dcID2DDFID(rightDataContainerID);
    DDF leftddf = sparkThread.getDDFManager().getDDF(leftDdfId);
    DDF rightddf = sparkThread.getDDFManager().getDDF(rightDdfId);
    
    DDF resultDDF;
    try {
      resultDDF = leftddf.join(rightddf, joinType, byColumns, byLeftColumns, byRightColumns);
      return new com.adatao.pa.spark.Utils.DataFrameResult(resultDDF);
    } catch (DDFException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
}
