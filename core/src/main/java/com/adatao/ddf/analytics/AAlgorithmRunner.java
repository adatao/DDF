package com.adatao.ddf.analytics;

import com.adatao.ddf.ADDFFunctionalGroupHandler;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithmRunner extends ADDFFunctionalGroupHandler implements IRunAlgorithms {

  public AAlgorithmRunner(DDF theDDF) {
    super(theDDF);
  }

  private void preprocess(IAlgorithm algorithm) {
    this.getDDF().getRepresentationHandler().get(algorithm.getExpectedDataType());
    algorithm.preprocess(this.getDDF());
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorithm algorithm) {
    preprocess(algorithm);
    return algorithm.run(this.getDDF());
  }
}
