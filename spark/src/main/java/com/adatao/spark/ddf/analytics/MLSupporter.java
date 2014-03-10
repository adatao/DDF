package com.adatao.spark.ddf.analytics;


import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import com.adatao.ddf.DDF;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;

public class MLSupporter extends com.adatao.ddf.analytics.MLSupporter {

  public MLSupporter(DDF theDDF) {
    super(theDDF);
  }


  /**
   * Override this to return the approriate DDF representation matching that specified in {@link ParamInfo}. The base
   * implementation simply returns the DDF.
   * 
   * @param paramInfo
   * @return
   */
  @Override
  protected Object convertDDF(ParamInfo paramInfo) {
    if (paramInfo.argMatches(RDD.class)) {
      // Yay, our target data format is an RDD!
      RDD<?> rdd = null;

      if (paramInfo.paramMatches(LabeledPoint.class)) {
        System.out.println("RDD<LabeledPoint>");

      } else if (paramInfo.paramMatches(Double[].class)) {
        System.out.println("RDD<Double[]>");

      } else if (paramInfo.paramMatches(Object.class)) {
        System.out.println("RDD<Object>");
      }

      return rdd;
    }

    else {
      return super.convertDDF(paramInfo);
    }
  }
}
