package com.adatao.ddf.analytics;

import java.util.Map;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;

/**
 * 
 * @author bhan
 * 
 */
public abstract class AAggregationHandler extends ADDFFunctionalGroupHandler
    implements IHandleAggregation {

  public AAggregationHandler(DDF theDDF) {
    super(theDDF);
  }

  public Map<String, Double[]> aggregate(String[] columnNames,
      String[] groupByColumns, String funcName) {
    if (AggregateFunction.fromString(funcName) == null)
      throw new UnsupportedOperationException();
    return aggregateImpl(columnNames, groupByColumns, funcName);
  }

  public double corr(String colA, String colB) {
    if (!this.getDDF().getSchemaHandler().getSchema().getColumn(colA)
        .isNumber())
      throw new UnsupportedOperationException();
    return corrImpl(colA, colB);
  }

  public abstract Map<String, Double[]> aggregateImpl(String[] columnNames,
      String[] groupByColumns, String funcName);

  public abstract double corrImpl(String colA, String colB);

  public enum AggregateFunction {
    mean, count, sum, min, max, median, variance, stddev;
    public static AggregateFunction fromString(String s) {
      if (s == null || s.length() == 0)
        return null;
      s = s.toUpperCase().trim();
      for (AggregateFunction t : values()) {
        if (s.equals(t.name()))
          return t;
      }
      return null;
    }

  }
}
