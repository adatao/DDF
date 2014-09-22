package com.adatao.ML.spark.clustering;

@SuppressWarnings("serial")
public class KMeansModel extends org.apache.spark.mllib.clustering.KMeansModel {

  public double wcss;
  public KMeansModel(double[][] clusterCenters) {
    super(clusterCenters);
  }
  public KMeansModel(double[][] clusterCenters, double wcss) {
    super(clusterCenters);
    this.wcss = wcss;
  }
  public double getWcss() {
    return wcss;
  }
  public void setWcss(double wcss) {
    this.wcss = wcss;
  }
}
