package com.adatao.ddf.analytics;


import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleBinning extends IHandleDDFFunctionalGroup {

//  public void Binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
//      boolean right);

  public double[] getQuantilesFromNumBins(Column column, int numBins);
  
  public double[] getIntervalsFromNumBins(Column column, int numBins);
}
