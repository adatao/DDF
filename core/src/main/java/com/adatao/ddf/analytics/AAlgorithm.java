package com.adatao.ddf.analytics;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithm implements IAlgorithm {

  private Class<?> mExpectedDataType;
  private IParameters mParameters;

  public AAlgorithm(Class<?> expectedDataType, IParameters params) {
    mExpectedDataType = expectedDataType;
    mParameters = params;
  }

  @Override
  public void setParameters(IParameters params) {
    mParameters = params;
  }

  @Override
  public IParameters getParameters() {
    return mParameters;
  }

  @Override
  public void setExpectedDataType(Class<?> expectedDataType) {
    mExpectedDataType = expectedDataType;
  }

  @Override
  public Class<?> getExpectedDataType() {
    return mExpectedDataType;
  }
}
