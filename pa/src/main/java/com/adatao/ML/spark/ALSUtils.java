package com.adatao.ML.spark;


import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.jblas.DoubleMatrix;

@SuppressWarnings("serial")
public class ALSUtils implements Serializable {

  public static DoubleMatrix toDoubleMatrix(JavaRDD<scala.Tuple2<Object, double[]>> componentMatrix, int m, int n) {
    DoubleMatrix prediction = new DoubleMatrix(m+1, n);
    List<scala.Tuple2<Object, double[]>> list = componentMatrix.collect();
    for (scala.Tuple2<Object, double[]> predicted : list) {
      for (int i = 0; i < n; i++) {
        prediction.put((Integer) predicted._1(), i, predicted._2()[i]);
      }
    }
    return prediction;
  }

  public static DoubleMatrix getUserFeatureMatrix(MatrixFactorizationModel model, int users, int features) {
    System.out.println(">>>>>>>>>>>IN getUserFeatureMatrix " + users + "and " + features);
    return toDoubleMatrix(model.userFeatures().toJavaRDD(), users, features);
  }

  public static DoubleMatrix getProductFeatureMatrix(MatrixFactorizationModel model, int products, int features) {
    System.out.println(">>>>>>>>>>>IN getProductFeatureMatrix " + products + "and " + features);
    return toDoubleMatrix(model.productFeatures().toJavaRDD(), products, features);
  }


  public static class ALSModel implements Serializable {

    int numFeatures;
    DoubleMatrix userFeatures;
    DoubleMatrix productFeatures;
    double rmse;


    public ALSModel(int numFeatures, DoubleMatrix userFeatures, DoubleMatrix productFeatures, double rmse) {
      super();
      this.numFeatures = numFeatures;
      this.userFeatures = userFeatures;
      this.productFeatures = productFeatures;
      this.rmse = rmse;
    }

    public double predict(int userId, int productId) {
      DoubleMatrix pUser = userFeatures.getRow(userId);
      DoubleMatrix pProduct = productFeatures.getRow(productId);
      return pUser.dot(pProduct);
    }

    public double[] predict(int userId, int[] candidateProducts) {
      int cands = candidateProducts.length;
      double[] predictedRatings = new double[cands];
      for (int i = 0; i < cands; i++) {
        predictedRatings[i] = predict(userId, candidateProducts[i]);
      }
      return predictedRatings;
    }
    
    public double getRmse() {
      return this.rmse;
    }
  }

}
