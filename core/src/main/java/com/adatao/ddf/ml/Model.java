package com.adatao.ddf.ml;


import com.adatao.ddf.exception.DDFException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.builder.EqualsBuilder;
/**
 */

public class Model implements IModel, Serializable {

  public static final Long serialVersionUID = 1L;

  private Object mRawModel;

  private transient Method mMethod;

  public Model(Object model) {
    mRawModel = model;
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
  }

  // Initialize mPredictMethod when needed, because
  // java.lang.reflect.Method is not serializable, so it cannot be passed to Spark RDD.map*
  private Method getPredictMethod() throws DDFException {
    if (mMethod == null) {
      mMethod = PredictMethod.fromModel(mRawModel);
    }
    return mMethod;
  }

  @Override
  public Double predict(double[] point) throws DDFException {

    try {
      Object result = this.getPredictMethod().invoke(mRawModel, point);
      if (result instanceof Double) {
        return (Double) result;
      } else if (result instanceof Integer) {
        return ((Integer) result).doubleValue();
      } else {
        throw new DDFException(
            String.format("Error predicting with model %s", this.getRawModel().getClass().getName()));
      }
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }
}
