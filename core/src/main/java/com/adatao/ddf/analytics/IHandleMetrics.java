package com.adatao.ddf.analytics;

import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMetrics extends IHandleDDFFunctionalGroup {

  public double R2Score(double[] yTrue, double[] yPred);
  
  public double R2Score(double[][] yTrue, double[][] yPred);
}
