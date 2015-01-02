package com.adatao.ML.spark.recommendation;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

public class ALSModel {
  public MatrixFactorizationModel mfModel;
  public double rmse;
  
  public ALSModel(MatrixFactorizationModel mfModel, double rmse) {
    super();
    this.mfModel = mfModel;
    this.rmse = rmse;
  }

  public double predict(int user, int product) {
    return mfModel.predict(user, product);
  }
  
  public double[] predict(int userId, int[] candidateProducts) {
    int cands = candidateProducts.length;
    double[] predictedRatings = new double[cands];
    for (int i = 0; i < cands; i++) {
      predictedRatings[i] = predict(userId, candidateProducts[i]);
    }
    return predictedRatings;
  }
  
  public MatrixFactorizationModel getMfModel() {
    return mfModel;
  }

  public void setMfModel(MatrixFactorizationModel mfModel) {
    this.mfModel = mfModel;
  }

  public double getRmse() {
    return this.rmse;
  }
}
