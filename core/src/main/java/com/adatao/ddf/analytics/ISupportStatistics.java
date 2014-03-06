package com.adatao.ddf.analytics;

import java.util.List;
import com.adatao.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface ISupportStatistics extends IHandleDDFFunctionalGroup {

  public Summary[] getSummary();
  
  public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

}
