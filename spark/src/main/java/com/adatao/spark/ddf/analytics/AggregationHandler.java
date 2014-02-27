package com.adatao.spark.ddf.analytics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
public class AggregationHandler extends AAggregationHandler {
  private static Logger logger = Logger.getLogger(AggregationHandler.class);

  public AggregationHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public double corr(String colA, String colB) {
    String sqlCmd = "Select corr(" + colA + ", " + colB + ") from "
        + this.getDDF().getTableName();
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
  public Map<String, Double[]> aggregate(String[] columnNames,
      String[] groupByColumns, String funcName) {
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
    if (sb.length() > 0)
      sb.setLength(sb.length() - 1);// remove the right most ","
    funcStr = sb.toString();
    String sqlCmd = "Select " + groupByStr + funcStr + " from " + tableName
        + " group by " + groupByStr;
    try {
      List<String> result = this.getManager().sql2txt(sqlCmd);
      return toAggregateResult(result, groupByColumns.length);
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return null;
  }

  private Map<String, Double[]> toAggregateResult(List<String> result, int len) {
    Map<String, Double[]> aggregateResult = new HashMap<String, Double[]>();
    for (String res : result) {
      int pos = StringUtils.ordinalIndexOf(res, "\t", len);
      String groupByColNames = res.substring(0, pos).replaceAll("\t", ",");
      String[] stats = res.substring(pos + 1).split("\t");
      Double[] statsDouble = new Double[stats.length];
      for (int i = 0; i < stats.length; i++) {
        statsDouble[i] = Utils.roundUp(Double.parseDouble(stats[i]));
      }
      aggregateResult.put(groupByColNames, statsDouble);
    }
    return null;
  }

  private String nameToFunction(String colName, String funcName) {
    switch (AggregateFunction.valueOf(funcName)) {
    case median:
      return String.format(" percentile_approx(%s, 0.5),", colName);
    default:
      return funcName + String.format(" (%s),", colName);
    }
  }

}
