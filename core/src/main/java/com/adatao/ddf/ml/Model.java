package com.adatao.ddf.ml;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import com.adatao.ddf.types.TJsonSerializable;
import com.adatao.ddf.types.TJsonSerializable$class;
import com.google.gson.Gson;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.MLClassMethods.PredictMethod;

/**
 */

public class Model implements IModel, Serializable {

  private static final long serialVersionUID = 8076703024981092021L;

  private Object mRawModel;

  private String mName;

//  private String mclazz;

  public Model(Object rawModel) {
    mRawModel = rawModel;
    mName = UUID.randomUUID().toString();
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public void setName(String name) {
    mName = name;
  }

  @Override
  public Double predict(double[] point) throws DDFException {

    PredictMethod predictMethod = new PredictMethod(this.getRawModel(), MLClassMethods.DEFAULT_PREDICT_METHOD_NAME,
        new Class<?>[] { point.getClass() });

    if (predictMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s",
          MLClassMethods.DEFAULT_PREDICT_METHOD_NAME));
    }

    Object prediction = predictMethod.instanceInvoke(point);

    if (prediction instanceof Double) {
      return (Double) prediction;

    } else if (prediction instanceof Integer) {
      return ((Integer) prediction).doubleValue();

    } else {
      throw new DDFException(String.format("Error getting prediction from model %s", this.getRawModel().getClass()
          .getName()));
    }

  }

  @Override
  public String toString() {

    Gson gson = new Gson();
    return gson.toJson(this.mRawModel);
  }
}
