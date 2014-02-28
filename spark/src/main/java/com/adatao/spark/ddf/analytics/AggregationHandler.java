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
  public double correlationImpl(String columnA, String columnB) {
    
    String sqlCmd = String.format("SELECT CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF().getTableName());
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
  public Map<String, Double[]> aggregateImpl(String[] ungroupedColumns, String[] groupedColumns, String funcName) {
    
    String tableName = this.getDDF().getTableName();
    String groupByStr, funcStr;
    StringBuffer sb = new StringBuffer("");
   
    for (String col : groupedColumns) {
      sb.append(String.format(" %s,", col));
    }
    groupByStr = sb.toString();
    if (sb.length() > 0) sb.setLength(sb.length() - 1);// remove the right most ","
    sb.setLength(0);

    for (String col : ungroupedColumns) {
      sb.append(getSqlFunctionCode(funcName, col));
      sb.append(",");
    }
    if (sb.length() > 0) sb.setLength(sb.length() - 1);// remove the right most ","
    funcStr = sb.toString();
    
    // e.g. select year, month, avg(depdelay), avg(arrdelay) from airline group by year, month;
    String sqlCmd = String.format("SELECT %s, %s FROM % GROUP BY %s", groupByStr, funcStr, tableName, groupByStr);
    try {
      List<String> result = this.getManager().sql2txt(sqlCmd);
      if (result.size() != 0) {
        return AggregationResult.newInstance(result, groupedColumns.length);
      }
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return null;
  }

  private String getSqlFunctionCode(String function, String column) {
    
    switch (AggregateFunction.valueOf(function.toUpperCase().trim())) {
      case MEDIAN:
        return String.format(" PERCENTILE_APPROX(%s, 0.5),", column);
      case MEAN:
        return String.format(" AVG(%s),", column);
      default:
        return String.format(" %s(%s),", function, column);
    }
  }
  
  @SuppressWarnings("serial")
  public static class AggregationResult extends HashMap<String, Double[]> {

    public AggregationResult() {
      super();
    }
    public static AggregationResult newInstance(List<String> sqlResult, int numFields) {
      
      Map<String, Double[]> aggregateResult = new HashMap<String, Double[]>();
      for (String res : sqlResult) {
        int pos = StringUtils.ordinalIndexOf(res, "\t", numFields);
        String groupByColNames = res.substring(0, pos).replaceAll("\t", ",");
        String[] stats = res.substring(pos + 1).split("\t");
        
        Double[] statsDouble = new Double[stats.length];
        
        for (int i = 0; i < stats.length; i++) {
          if (!"null".equalsIgnoreCase(stats[i])) {
            statsDouble[i] = Double.NaN;
          }
          else {
            statsDouble[i] = Utils.roundUp(Double.parseDouble(stats[i]));
          }
        }
        aggregateResult.put(groupByColNames, statsDouble);
      }
      return (AggregationResult)aggregateResult;
    }
    
  }

}
