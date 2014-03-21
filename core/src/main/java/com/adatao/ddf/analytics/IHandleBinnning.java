package com.adatao.ddf.analytics;


import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleBinnning extends IHandleDDFFunctionalGroup {

  public void Binning(String column, String binningType, int numBins, Double[] breaks, boolean includeLowest,
      boolean right);

  public Double[] getQuantilesFromNumBins(Column column, Double[] breaks, int numBins);
  
  public Double[] getIntervalsFromNumBins(Column column, Double[] breaks, int numBins);
}
