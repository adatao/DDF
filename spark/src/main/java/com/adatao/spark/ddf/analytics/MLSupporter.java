package com.adatao.spark.ddf.analytics;


import com.adatao.ddf.exception.DDFException;
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
  @SuppressWarnings("unchecked")
  @Override
  protected Object convertDDF(ParamInfo paramInfo) throws DDFException{
    if (paramInfo.argMatches(RDD.class)) {
      // Yay, our target data format is an RDD!
      RDD<?> rdd = null;

      if (paramInfo.paramMatches(LabeledPoint.class)) {
        rdd = (RDD<LabeledPoint>)this.getDDF().getRepresentationHandler().get(LabeledPoint.class);
        System.out.println("RDD<LabeledPoint>");
      } else if (paramInfo.paramMatches(double[].class)) {
        rdd = (RDD<double[]>)this.getDDF().getRepresentationHandler().get(double[].class);
        System.out.println("RDD<Double[]>");
      } else if (paramInfo.paramMatches(Object.class)) {
        rdd= (RDD<Object[]>) this.getDDF().getRepresentationHandler().get(Object[].class);
        System.out.println("RDD<Object>");
      }
      return rdd;
    }

    else {
      return super.convertDDF(paramInfo);
    }
  }
}
