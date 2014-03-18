package com.adatao.ddf.ml;


import com.adatao.ddf.exception.DDFException;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 */

public class Model implements IModel, Serializable {

  public static final Long serialVersionUID = 1L;

  private Object mModel;

  private transient Method mPredictMethod;

  public Model(Object model) {
    mModel = model;
  }

  @Override
  public Object getRawModel() {
    return mModel;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Model)) return false;

    if (this.getRawModel().getClass() != ((Model) other).getRawModel().getClass()) return false;
    // TO DO: PARSE PARAMETERS FROM MODEL FOR EQUALS IMPLEMENTATION

    return true;
  }

  // Initialize mPredictMethod when needed, because
  // java.lang.reflect.Method is not serializable, so it cannot be passed to Spark RDD.map*
  private Method getPredictMethod() throws DDFException {
    if (mPredictMethod == null) {
      mPredictMethod = MLPredictMethod.get(mModel);
    }
    return mPredictMethod;
  }

  @Override
  public Double predict(double[] point) throws DDFException {

    try {
      Object result = this.getPredictMethod().invoke(this.getRawModel(), point);
      if (result instanceof Double) {
        return (Double) result;
      } else if (result instanceof Integer) {
        return ((Integer) result).doubleValue();
      } else {
        throw new DDFException(
            String.format("Error getting prediction for %s", this.getRawModel().getClass().getName()));
      }
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }
}
