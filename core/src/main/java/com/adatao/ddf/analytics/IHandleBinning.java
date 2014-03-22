package com.adatao.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleBinning extends IHandleDDFFunctionalGroup {

  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException;

}
