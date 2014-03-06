/**
 * 
 */
package com.adatao.ddf.facades;


import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.ISupportML;
import com.adatao.ddf.exception.DDFException;

/**
 * A helper class to group together the various ML functions that would otherwise crowd up DDF.java
 */
public class MLFacade implements ISupportML {

  private DDF mDDF;
  private ISupportML mMLSupporter;


  public MLFacade(DDF ddf, ISupportML mlSupporter) {
    mDDF = ddf;
    mMLSupporter = mlSupporter;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public ISupportML getMLSupporter() {
    return mMLSupporter;
  }

  public void setMLSupporter(ISupportML mlSupporter) {
    mMLSupporter = mlSupporter;
  }

  @Override
  public IModel train(IAlgorithm algorithm, Object... params) throws DDFException {
    return this.getMLSupporter().train(algorithm, params);
  }

  public IModel train(String algorithmNameOrClassName, Object... params) throws DDFException {
    return this.getMLSupporter().train(algorithmNameOrClassName, params);
  }

  public IModel kMeans(int numCentroids, int maxIters, int runs, String initMode) throws DDFException {
    return this.train("kmeans", numCentroids, maxIters, runs, initMode);
  }

}
