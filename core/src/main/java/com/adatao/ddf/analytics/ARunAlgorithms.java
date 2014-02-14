package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public abstract class ARunAlgorithms implements IRunAlgorithms {

  private void preprocess(IAlgorihtm algorihtm, DDF ddf) {
    ddf.getRepresentationHandler().getRepresentation(algorihtm.getElementType());
  }

  @Override
  public IAlgorithmOutputModel run(IAlgorihtm algorihtm, DDF ddf) {

    preprocess(algorihtm, ddf);

    return algorihtm.run(ddf);
  }
}
