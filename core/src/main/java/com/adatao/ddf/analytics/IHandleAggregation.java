package com.adatao.ddf.analytics;


import java.util.List;
import com.adatao.ddf.IHandleDDFFunctionalGroup;
import com.adatao.ddf.analytics.AggregationHandler.AggregateField;
import com.adatao.ddf.analytics.AggregationHandler.AggregationResult;
import com.adatao.ddf.exception.DDFException;

public interface IHandleAggregation extends IHandleDDFFunctionalGroup {

  // public double computeCorrelation(String columnA, String columnB);

  public AggregationResult aggregate(List<AggregateField> fields) throws DDFException;

}
