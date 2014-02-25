package com.adatao.ddf.analytics;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class AAlgorithmRunner extends ADDFFunctionalGroupHandler implements IRunAlgorithms {

  public AAlgorithmRunner(ADDFManager manager) {
    super(manager);
  }

  private void preprocess(IAlgorithm algorithm) {
    this.getManager().getDDF().getRepresentationHandler().get(algorithm.getExpectedDataType());
    algorithm.preprocess(this.getManager().getDDF());
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorithm algorithm) {
    preprocess(algorithm);
    return algorithm.run(this.getManager().getDDF());
  }
}
