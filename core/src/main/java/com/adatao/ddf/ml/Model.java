package com.adatao.ddf.ml;


import java.io.Serializable;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.MLClassMethods.PredictMethod;

/**
 */

public class Model implements IModel, Serializable {

  private static final long serialVersionUID = 8076703024981092021L;

  private Object mRawModel;


  public Model(Object rawModel) {
    mRawModel = rawModel;
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
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
}
