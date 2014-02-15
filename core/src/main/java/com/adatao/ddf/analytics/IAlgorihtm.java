package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public interface IAlgorihtm {

  public void preprocess(DDF ddf);

  public IAlgorithmOutputModel run(DDF ddf);

  public IParameters getParameters();

  public Class<?> getElementType();
}
