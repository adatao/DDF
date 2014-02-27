package com.adatao.ddf.analytics;

import java.util.Map;

import com.adatao.ddf.IHandleDDFFunctionalGroup;

public interface IHandleAggregation extends IHandleDDFFunctionalGroup {
  
  public Map<String, Double[]> aggregate(String[] columnNames, String[] groupByColumns, String funcName);
  
  public double corr(String colA, String colB);
  
}