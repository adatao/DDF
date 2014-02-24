package com.adatao.ddf.analytics;

import com.adatao.ddf.IHandleDDFFunctionalGroup;

public interface IComputeBasicStatistics extends IHandleDDFFunctionalGroup {

  public Summary[] getSummary();

}
