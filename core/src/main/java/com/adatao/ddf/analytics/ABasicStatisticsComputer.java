package com.adatao.ddf.analytics;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;

/**
 * 
 * @author bhan
 * 
 */
public abstract class ABasicStatisticsComputer extends ADDFFunctionalGroupHandler implements IComputeBasicStatistics {

  public ABasicStatisticsComputer(DDF theDDF) {
    super(theDDF);
  }

  private Summary[] basicStats;

  protected abstract Summary[] getSummaryImpl();

  public Summary[] getSummary() {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }

}
