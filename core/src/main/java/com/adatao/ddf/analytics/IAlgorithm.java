package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public interface IAlgorithm {

  public void preprocess(DDF ddf);

  public IAlgorithmOutputModel run(DDF ddf);

  public void setParameters(IParameters params);

  public IParameters getParameters();

  public void setExpectedDataType(Class<?> expectedDataType);

  public Class<?> getExpectedDataType();
}
