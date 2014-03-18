package com.adatao.ddf.ml;

import com.adatao.ddf.exception.DDFException;

import java.lang.reflect.Method;

/**
 */
public class PredictMethod {

  private Object mModel;

  private Method mPredictMethod;

  private static final String DEFAUL_PREDICT_METHOD_NAME = "predict";

  private static final Class<?> DEFAULT_PREDICT_TYPE_PARAM = double[].class;

  public PredictMethod(Object model) throws DDFException {
    mModel = model;
    try {
      mPredictMethod = this.mModel.getClass().getMethod(DEFAUL_PREDICT_METHOD_NAME, DEFAULT_PREDICT_TYPE_PARAM);
    } catch (NoSuchMethodException e) {
      throw new DDFException(String.format("Error: Cannot get predict method for model %s", mModel.getClass().getName()));
    }
  }

  public Method getMethod() {
    return mPredictMethod;
  }

  public static Method fromModel(Object mModel) throws DDFException {
    return (new PredictMethod(mModel)).getMethod();
  }
}
