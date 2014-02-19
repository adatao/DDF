package com.adatao.ddf.analytics;

import java.util.List;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;
import com.google.common.collect.Lists;

public abstract class ABasicStatisticsComputer extends
    ADDFFunctionalGroupHandler implements IComputeBasicStatistics {

  public ABasicStatisticsComputer(ADDFManager theDDFManager) {
    super(theDDFManager);
    // TODO Auto-generated constructor stub
  }

  private Summary[] basicStats;
  protected abstract Summary[] getSummaryImpl();

  public Summary[] getSummary() {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }

}
