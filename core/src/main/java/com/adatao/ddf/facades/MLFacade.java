/**
 * 
 */
package com.adatao.ddf.facades;


import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.IModel;
import com.adatao.ddf.ml.ISupportML;

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
  public DDF applyModel(IModel model) throws DDFException {
    return this.getMLSupporter().applyModel(model);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels) throws DDFException {
    return this.getMLSupporter().applyModel(model, hasLabels);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    return this.getMLSupporter().applyModel(model, hasLabels, includeFeatures);
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

  public IModel logisticRegressionWithSGD(int[] featureColumnIndexes, int targetColumnIndex, int stepSize,
      double miniBatchFraction) throws DDFException {
    return this.train("LogisticRegressionWithSGD", featureColumnIndexes, targetColumnIndex, stepSize, miniBatchFraction);
  }

  public Double[][] getConfusionMatrix(IModel model, double threshold) throws DDFException {
    return this.getMLSupporter().getConfusionMatrix(model, threshold); 
  }
}
