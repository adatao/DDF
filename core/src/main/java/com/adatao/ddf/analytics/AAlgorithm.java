package com.adatao.ddf.analytics;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithm implements IAlgorihtm{

  private Class<?> mElementType;
  private IParameters mParameters;

  public AAlgorithm(Class<?> elementType, IParameters params) {

    mElementType= elementType;
    mParameters= params;

  }
  public IParameters getParameters() {

    return mParameters;
  }

  public void setParameters(IParameters params) {

    mParameters= params;
  }

  public Class<?> getElementType() {

    return mElementType;
  }

  public void setElementType(Class<?> elementType) {

    mElementType= elementType;
  }

}
