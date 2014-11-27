package com.adatao.ML.spark.clustering;

import org.apache.spark.mllib.linalg.Vector;
@SuppressWarnings("serial")
public class KMeansModel extends org.apache.spark.mllib.clustering.KMeansModel {

  public double wcss;
  public KMeansModel(Vector[] clusterCenters) {
    super(clusterCenters);
  }
  public KMeansModel(Vector[] clusterCenters, double wcss) {
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
