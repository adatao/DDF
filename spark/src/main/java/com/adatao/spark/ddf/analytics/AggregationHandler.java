package com.adatao.spark.ddf.analytics;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.AAggregationHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils;
/**
 * 
 * @author bhan
 *
 */
public class AggregationHandler extends AAggregationHandler{
  private static Logger logger = Logger.getLogger(AggregationHandler.class);

  public AggregationHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public double corr(String colA, String colB) {
    String sqlCmd = "Select corr(" + colA + ", " + colB + ") from "+ this.getDDF().getTableName();
    List<String> rs;
    try {
      rs = this.getManager().sql2txt(sqlCmd);
      return Utils.roundUp(Double.parseDouble(rs.get(0)));
    } catch (DDFException e) {
      logger.error("Unable to query from " + this.getDDF().getTableName(), e);
    }
    return 0;
  }
  @Override
  public Map<String, Map<String, Double>> aggregate(String[] columnNames, String[] groupByColumns, String funcName) {
    String tableName = this.getDDF().getTableName();
    String groupByStr, funcStr;
    StringBuffer sb = new StringBuffer("");
    for (String col : groupByColumns) {
      sb.append(String.format(" %s,", col));
    }
    groupByStr = sb.toString();
    sb.setLength(0);
    
    for (String col : columnNames) {
      sb.append(nameToFunction(col, funcName));
      sb.append(",");
    }
    if (sb.length()>0) sb.setLength(sb.length()-1);// remove the right most ","
    funcStr = sb.toString();
    String sqlCmd = "Select " + groupByStr + funcStr + " from " + tableName + " group by " + groupByStr;
    try {
      List<String> result = this.getManager().sql2txt(sqlCmd);
      return toAggregateResult(result);
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return null;
  }

  private Map<String, Map<String, Double>> toAggregateResult(List<String> result) {
    // TODO Auto-generated method stub
    return null;
  }
  private String nameToFunction(String col, String funcName) {
    // TODO Auto-generated method stub
    return null;
  }

  
  
}
