package com.adatao.spark.ddf.ml;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandleRepresentations.IGetResult;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.AMLMetricsSupporter;
import com.adatao.spark.ddf.SparkDDF;

public class MLMetricsSupporter extends AMLMetricsSupporter {

  @Override
  /*
   * input: prediction DDF
   * input: meanYtrue
   * 
   * output: r2score in double 
   * 
   * (non-Javadoc)
   * @see com.adatao.ddf.ml.AMLMetricsSupporter#r2score(com.adatao.ddf.DDF, double)
   */
  public double r2score(DDF predictionDDF, double meanYTrue) throws DDFException {
    IGetResult gr = ((SparkDDF) predictionDDF).getJavaRDD(double[].class, double.class, double[].class);
    JavaRDD resultRDD = ((JavaRDD<double[]>) gr.getObject());

    double[] result = (double[]) resultRDD.map(new MetricsMapperR2(meanYTrue)).reduce(new MetricsReducerR2());
    double sstot = result[0];
    double ssres = result[1];
    if (sstot == 0) {
      return 1;
    } else {
      return 1 - (ssres / sstot);
    }
  }


  public static class MetricsMapperR2 extends Function<double[], double[]> {
    private static final long serialVersionUID = 1L;
    public double meanYTrue = -1.0;

    public MetricsMapperR2(double _meanYTrue) throws DDFException {
      this.meanYTrue = _meanYTrue;
    }

    public double[] call(double[] input) throws Exception {
      double[] outputRow = null;

      if (input instanceof double[] && input.length > 1) {
        double yTrue = input[0];
        double yPredict = input[1];
        outputRow[0] = (yTrue - meanYTrue) * (yTrue - meanYTrue);
        outputRow[1] = outputRow[0] = (yTrue - yPredict) * (yTrue - yPredict);
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }

  public static class MetricsReducerR2 extends Function2<double[], double[], double[]> {
    private static final long serialVersionUID = 1L;


    public MetricsReducerR2() throws DDFException {}

    @Override
    public double[] call(double[] arg0, double[] arg1) throws Exception {
      double[] outputRow = null;
      if (arg0 instanceof double[] && arg0.length > 1) {
        outputRow[0] = arg0[0] + arg1[0];
        outputRow[1] = arg0[1] + arg1[1];
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }


  @Override
  public DDF residuals(DDF predictionDDF) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object roc(DDF predictionDDF) {
    // TODO Auto-generated method stub
    return null;
  }

}
