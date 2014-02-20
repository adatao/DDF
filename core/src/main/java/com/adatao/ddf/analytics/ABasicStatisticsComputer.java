package com.adatao.ddf.analytics;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;
/**
 * 
 * @author bhan
 *
 */
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
