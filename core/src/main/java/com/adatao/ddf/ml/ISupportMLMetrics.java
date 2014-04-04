package com.adatao.ddf.ml;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface ISupportMLMetrics extends IHandleDDFFunctionalGroup {
  
  public double r2score(DDF predictionDDF, double meanYTrue) throws DDFException;
  public DDF residuals(DDF predictionDDF);
  public Object roc(DDF predictionDDF);

}
