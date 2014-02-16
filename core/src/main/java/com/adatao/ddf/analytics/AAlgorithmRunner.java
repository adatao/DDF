package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithmRunner implements IRunAlgorithms {

  private void preprocess(IAlgorithm algorithm, DDF ddf) {
    ddf.getRepresentationHandler().getRepresentation(algorithm.getExpectedDataType());
    algorithm.preprocess(ddf);
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorithm algorithm, DDF ddf) {
    preprocess(algorithm, ddf);

    return algorithm.run(ddf);
  }
}
