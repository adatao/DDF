package com.adatao.ddf.analytics;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;
/**
 * 
 * @author bhan
 *
 */
public abstract class AAggregationHandler extends ADDFFunctionalGroupHandler implements IHandleAggregation {

  public AAggregationHandler(DDF theDDF) {
    super(theDDF);
  }
  
  public double aggregate(int[] columnIndices, String func) {
    return aggregate(getColumnNames(columnIndices), func);
  }
  
  private int[] getColumnNames(int[] columnIndices) {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean isSupported(String[] columnNames, String funcName) {
    if (AggregateFunction.fromString(funcName) == null) return false;
    // TODO check columnType must be number
    return true;
  }
  
  public boolean isValid(String[] columnNames, String funcName) {
    return ( "corr".equalsIgnoreCase(funcName) && columnNames.length !=2);
    // TODO check columnNames exist
  }
  
  public enum AggregateFunction {
    mean, sum, min, max, median, variance, stdev, corr;
    public static AggregateFunction fromString(String s) {
      if (s == null || s.length() == 0) return null;
      s = s.toUpperCase().trim();
      for (AggregateFunction t : values()) {
        if (s.equals(t.name())) return t;
      }
      return null;
  }
  

  }
}
