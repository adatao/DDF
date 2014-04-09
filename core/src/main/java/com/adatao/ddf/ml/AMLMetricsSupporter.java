package com.adatao.ddf.ml;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;

public class AMLMetricsSupporter implements ISupportMLMetrics {

  public AMLMetricsSupporter(DDF theDDF) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public DDF getDDF() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDDF(DDF theDDF) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public double r2score(DDF predictionDDF, double meanYTrue) throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DDF residuals(DDF predictionDDF) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object roc(DDF predictionDDF) {
    // TODO Auto-generated method stub
    return null;
  }

}
