package com.adatao.ddf.ml;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;
import com.adatao.ddf.ml.RocMetric;

public interface ISupportMLMetrics extends IHandleDDFFunctionalGroup {
  
  public double r2score(DDF predictionDDF, double meanYTrue) throws DDFException;
  public DDF residuals(DDF predictionDDF) throws DDFException;
  public RocMetric roc(DDF predictionDDF, int alpha_length) throws DDFException;

}
