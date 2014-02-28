package com.adatao.ddf.analytics;

import java.util.Map;

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

  public Map<String, Double[]> aggregate(String[] ungroupedColumns, String[] groupedColumns, String funcName) {
    if (AggregateFunction.fromString(funcName) == null) throw new UnsupportedOperationException();
    return aggregateImpl(ungroupedColumns, groupedColumns, funcName);
  }

  public double computeCorrelation(String columnA, String columnB) {
    if (!this.getDDF().getColumn(columnA).isNumeric()) throw new UnsupportedOperationException();
    return correlationImpl(columnA, columnB);
  }

  public abstract Map<String, Double[]> aggregateImpl(String[] ungroupedColumns, String[] groupedColumns,
      String funcName);

  public abstract double correlationImpl(String columnA, String columnB);

  public enum AggregateFunction {
    MEAN , COUNT, SUM, MIN, MAX, MEDIAN, VARIANCE, STDDEV;
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
