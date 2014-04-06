package com.adatao.ddf.ml;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.RocMetric;

public class AMLMetricsSupporter implements ISupportMLMetrics {

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
  public RocMetric roc(DDF predictionDDF, int alpha_length) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

}
