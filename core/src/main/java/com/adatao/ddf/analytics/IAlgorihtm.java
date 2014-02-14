package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public interface IAlgorihtm {

  public IAlgorithmOutputModel run(DDF theDDF);

  public IParameters getParameters();

  public Class<?> getElementType();
}
