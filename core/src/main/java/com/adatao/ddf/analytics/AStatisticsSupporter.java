package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

/**
 * 
 * @author bhan
 * 
 */
public abstract class AStatisticsSupporter extends ADDFFunctionalGroupHandler implements ISupportStatistics {

  public AStatisticsSupporter(DDF theDDF) {
    super(theDDF);
  }

  private Summary[] basicStats;

  protected abstract Summary[] getSummaryImpl();

  public Summary[] getSummary() {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }

}
