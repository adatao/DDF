/**
 * 
 */
package com.adatao.ddf.misc;


import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.ISupportML;
import com.adatao.ddf.exception.DDFException;

/**
 * A helper class to group together the various ML functions that would otherwise crowd up DDF.java
 */
public class MLDelegate implements ISupportML {

  private DDF mDDF;
  private ISupportML mMLSupporter;


  public MLDelegate(DDF ddf, ISupportML mlSupporter) {
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
  public IModel train(IAlgorithm algorithm, int[] featureColumnIndexes, Object... params) throws DDFException {
    return this.getMLSupporter().train(algorithm, featureColumnIndexes, params);
  }

  @Override
  public IModel train(IAlgorithm algorithm, int[] featureColumnIndexes, int targetColumnIndex, Object... params)
      throws DDFException {
    return this.getMLSupporter().train(algorithm, featureColumnIndexes, params);
  }

  @Override
  public IModel train(String algorithmNameOrClassName, int[] featureColumnIndexes, Object... params)
      throws DDFException {
    return this.getMLSupporter().train(algorithmNameOrClassName, featureColumnIndexes, params);
  }

  @Override
  public IModel train(String algorithmNameOrClassName, int[] featureColumnIndexes, int targetColumnIndex,
      Object... params) throws DDFException {
    return this.getMLSupporter().train(algorithmNameOrClassName, featureColumnIndexes, targetColumnIndex, params);
  }

  public IModel kMeans(int[] featureColumnIndexes, int numCentroids, int maxIters, int runs, String initMode)
      throws DDFException {
    return this.train("kmeans", featureColumnIndexes, numCentroids, maxIters, runs, initMode);
  }

  public IModel linearRegressionWithSGD(int[] featureColumnIndexes, int targetColumnIndex, int stepSize,
      double miniBatchFraction) throws DDFException {
    return this.train("LinearRegressionWithSGD", featureColumnIndexes, targetColumnIndex, stepSize, miniBatchFraction);
  }
}
