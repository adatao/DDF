package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class ARunAlgorithms implements IRunAlgorithms {

  private void preprocess(IAlgorithm algorihtm, DDF ddf) {

    algorihtm.preprocess(ddf);
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorithm algorihtm, DDF ddf) {

    preprocess(algorihtm, ddf);

    return algorihtm.run(ddf);
  }
}
