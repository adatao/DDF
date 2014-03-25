package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;
import com.adatao.ddf.Factor;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.content.Schema.ColumnType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

public abstract class ABinningHandler extends ADDFFunctionalGroupHandler implements IHandleBinning {

  protected double[] breaks;
  
  public ABinningHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }
  
  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException {

    DDF newddf = binningImpl(column, binningType, numBins, breaks, includeLowest, right);
    //Factor<String> factor = (Factor<String>) newddf.setAsFactor(column);
    
    return newddf;
  }

  public abstract DDF binningImpl(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException;
  
  public enum BinningType {
    CUSTOM, EQUAlFREQ, EQUALINTERVAL;
    
    public static BinningType get(String s) {
      if (s == null || s.length() == 0) return null;

      for (BinningType type : values()) {
        if (type.name().equalsIgnoreCase(s)) return type;
      }

      return null;
    }
  }
}
