package com.adatao.spark.ddf.analytics;

import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.MLSupporter;
import com.adatao.ddf.analytics.MLSupporter.Model;
import com.adatao.ddf.exception.DDFException;
import org.apache.spark.mllib.regression.LinearRegressionModel;

public class LogisticRegressionModel extends Model {
  public LogisticRegressionModel(LinearRegressionModel mlibModel) {
    super(mlibModel);
  }
  public DDF predict(Object data, DDF ddf) {
    return null;
  }
  
  public boolean isSupervisedAlgorithmModel() {
    return true;
  }
}
