package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithmRunner implements IRunAlgorithms {

  private void preprocess(IAlgorithm algorihtm, DDF ddf) {
    ddf.getRepresentationHandler().getRepresentation(algorihtm.getExpectedDataType());
    algorihtm.preprocess(ddf);
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorithm algorihtm, DDF ddf) {
    preprocess(algorihtm, ddf);

    return algorihtm.run(ddf);
  }
}
