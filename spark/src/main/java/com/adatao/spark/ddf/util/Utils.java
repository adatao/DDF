package com.adatao.spark.ddf.util;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
public class Utils {

  public static Vector arrayDoubleToVector(double[] arrDouble) {
    return Vectors.dense(arrDouble); // a dense vector
  }
}


