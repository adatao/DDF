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
  public IModel train(String trainMethodName, Object... params) throws DDFException {
    return this.getMLSupporter().train(trainMethodName, params);
  }

  @Override
  public IModel train(String trainMethodName, int[] featureColumnIndexes, Object... params) throws DDFException {
    return this.getMLSupporter().train(trainMethodName, featureColumnIndexes, params);
  }

  @Override
  public IModel train(String trainMethodName, int targetColumnIndex, int[] featureColumnIndexes, Object... params)
      throws DDFException {
    return this.getMLSupporter().train(trainMethodName, targetColumnIndex, featureColumnIndexes, params);
  }

  @Override
  public IModel train(String trainMethodName, String[] featureColumnNames, Object... params) throws DDFException {
    return this.getMLSupporter().train(trainMethodName, featureColumnNames, params);
  }

  @Override
  public IModel train(String trainMethodName, String targetColumnName, String[] featureColumnNames, Object... params)
      throws DDFException {
    return this.getMLSupporter().train(trainMethodName, targetColumnName, featureColumnNames, params);
  }



  // //// Convenient facade ML algorithm names //////

  public IModel kMeans(int[] featureColumnIndexes, int numCentroids, int maxIters, int runs, String initMode)
      throws DDFException {
    return this.train("kmeans", featureColumnIndexes, numCentroids, maxIters, runs, initMode);
  }

  public IModel linearRegressionWithSGD(int[] featureColumnIndexes, int targetColumnIndex, int stepSize,
      double miniBatchFraction) throws DDFException {
    return this.train("LinearRegressionWithSGD", featureColumnIndexes, targetColumnIndex, stepSize, miniBatchFraction);
  }

}
