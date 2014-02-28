package com.adatao.ddf.analytics;

import java.util.Map;

import com.adatao.ddf.IHandleDDFFunctionalGroup;

public interface IHandleAggregation extends IHandleDDFFunctionalGroup {

  public Map<String, Double[]> aggregate(String[] ungroupedColumns, String[] groupedColumns, String aggregationFunction);

  public double computeCorrelation(String columnA, String columnB);

}