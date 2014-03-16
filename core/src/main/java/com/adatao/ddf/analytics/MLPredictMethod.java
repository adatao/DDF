package com.adatao.ddf.analytics;

import com.adatao.ddf.exception.DDFException;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 */

public class MLPredictMethod {

  private Object mModel;

  private Method mPredictMethod;

  private static final String DEFAUL_PREDICT_METHOD_NAME = "predict";

  private List<Class<?>> mPreferedParamType = new ArrayList<Class<?>>();

  public MLPredictMethod(Object model) throws DDFException {
    mModel = model;
    mPreferedParamType.add(double[].class);
    mPredictMethod = this.getPredictMethod();
  }

  private Method getPredictMethod() throws DDFException {
    Method theMethod = null;
    for(Class<?> paramType: this.mPreferedParamType) {
      try {
        theMethod = this.mModel.getClass().getMethod(DEFAUL_PREDICT_METHOD_NAME, paramType);
        break;
      } catch(NoSuchMethodException e) {
        continue;
      }
    }
    if(theMethod == null) throw new DDFException(String.format("Error: Cannot get predict method for %s", mModel.getClass().getName()));
    else return theMethod;
  }

  public Method getMethod() {
    return mPredictMethod;
  }

  public Class<?> getPredictReturnType() {
    Class<?> returnType = this.getMethod().getReturnType();

    if(returnType == double.class) return Double.class;
    else if(returnType == int.class) return Integer.class;
    else return returnType;
  }

  public Class<?> getInputType() throws DDFException {
    Class<?>[] inputTypes = this.getMethod().getParameterTypes();

    if(inputTypes.length > 1)
      throw new DDFException(String.format("Error: Cannot get predict method for %s", mModel.getClass().getName()));
    else return inputTypes[0];
  }
}
