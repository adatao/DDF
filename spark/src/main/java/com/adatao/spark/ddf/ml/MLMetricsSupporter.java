package com.adatao.spark.ddf.ml;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandleRepresentations.IGetResult;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.AMLMetricsSupporter;
import com.adatao.ddf.ml.RocMetric;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.content.RepresentationHandler;

public class MLMetricsSupporter extends AMLMetricsSupporter {
  
  private Boolean sIsNonceInitialized = false;
  
  @Override
  /*
   * input: prediction DDF input: meanYtrue
   * 
   * output: r2score in double
   * 
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.ml.AMLMetricsSupporter#r2score(com.adatao.ddf.DDF, double)
   */
  public double r2score(DDF predictionDDF, double meanYTrue) throws DDFException {
    IGetResult gr = ((SparkDDF) predictionDDF).getJavaRDD(double[].class, double.class, double[].class);
    JavaRDD resultRDD = ((JavaRDD<double[]>) gr.getObject());

    double[] result = (double[]) resultRDD.map(new MetricsMapperR2(meanYTrue)).reduce(new MetricsReducerR2());
    if(result == null || result.length == 0) {
    	throw new DDFException("R2score result returns null");
    }
    double sstot = result[0];
    double ssres = result[1];
    
    if (sstot == 0) {
      return 1;
    } else {
      return 1 - (ssres / sstot);
    }
  }


  public static class MetricsMapperR2 implements Function<double[], double[]> {
    private static final long serialVersionUID = 1L;
    public double meanYTrue = -1.0;


    public MetricsMapperR2(double _meanYTrue) throws DDFException {
      this.meanYTrue = _meanYTrue;
    }

    public double[] call(double[] input) throws Exception {
      double[] outputRow = new double[2];

      if (input instanceof double[] && input.length > 1) {
        double yTrue = input[0];
        double yPredict = input[1];
        outputRow[0] = (yTrue - meanYTrue) * (yTrue - meanYTrue);
        outputRow[1] = (yTrue - yPredict) * (yTrue - yPredict);
        
        
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }

  public static class MetricsReducerR2 implements Function2<double[], double[], double[]> {
    private static final long serialVersionUID = 1L;


    public MetricsReducerR2() throws DDFException {}

    @Override
    public double[] call(double[] arg0, double[] arg1) throws Exception {
      double[] outputRow = new double[2];
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
  public DDF residuals(DDF predictionDDF) throws DDFException {
    IGetResult gr = ((SparkDDF) predictionDDF).getJavaRDD(double[].class, double.class, double[].class);
    JavaRDD predictionRDD = ((JavaRDD<double[]>) gr.getObject());

    JavaRDD<double[]> result = predictionRDD.map(new MetricsMapperResiduals());

    if(result== null) System.err.println(">> javaRDD result of MetricMapper residuals is null");
    if(predictionDDF.getManager()== null) System.err.println(">> predictionDDF.getManager() is null");
    if(result.rdd()== null) System.err.println(">> result.rdd() is null");
    if(predictionDDF.getNamespace()== null) System.err.println(">> predictionDDF.getNamespace() is null");
    if(predictionDDF.getSchema()== null) System.err.println(">> predictionDDF.getSchema() is null");
    if(predictionDDF.getName()== null) System.err.println(">> predictionDDF.getName() is null");
    
    Schema schema = new Schema("doubble residuals");
    DDF residualDDF = new SparkDDF(predictionDDF.getManager(), result.rdd(), double[].class, predictionDDF.getNamespace(), null, schema);
    
    if(residualDDF == null) System.err.println(">>>>>>>>>>>.residualDDF is null");
    
    if(residualDDF != null) predictionDDF.getManager().addDDF(residualDDF);
//        predictionDDF.getManager().newDDF(result, new Class[] { Array.class, double[].class}, predictionDDF.getNamespace(), predictionDDF.getName(), predictionDDF.getSchema());
    return residualDDF;
  }
  
  public static class MetricsMapperResiduals implements Function<double[], double[]> {
    private static final long serialVersionUID = 1L;

    public MetricsMapperResiduals() throws DDFException {
    }

    public double[] call(double[] input) throws Exception {
      double[] outputRow = new double[1];

      if (input instanceof double[] && input.length > 1) {
        double yTrue = input[0];
        double yPredict = input[1];
        outputRow[0] = outputRow[0] = (yTrue - yPredict);
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }

  @Override
  /*
   * input expected RDD[double[][]]
   * (non-Javadoc)
   * @see com.adatao.ddf.ml.AMLMetricsSupporter#roc(com.adatao.ddf.DDF, int)
   */
  public RocMetric roc(DDF predictionDDF, int alpha_length) throws DDFException {
    
	IGetResult gr = ((SparkDDF) predictionDDF).getRDD(double[].class, double.class);
    RDD<double[]> predictionRDD =  (RDD<double[]>) gr.getObject();
    //convert from rdd[double[]] to rdd[LabeledPoint]
    RDD<LabeledPoint[]> newrdd = RepresentationHandler.arrayDoubleToArrayLabeledPoints(predictionRDD);
    
    ROCComputer rc = new ROCComputer();
    return (RocMetric) (rc.ROC(newrdd, alpha_length));
    
  }
  
  public MLMetricsSupporter(DDF theDDF) {
    super(theDDF);
    this.initialize();
  }

  private void initialize() {
    if (sIsNonceInitialized) return;

    synchronized (sIsNonceInitialized) {
      if (sIsNonceInitialized) return;
      sIsNonceInitialized = true;

      this.initializeConfiguration();
    }
  }

  /**
   * Optional: put in any hard-coded mapping configuration here
   */
  private void initializeConfiguration() {
    // if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans"))) {
    // Config.set(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans",
    // String.format("%s#%s", MLSupporter.class.getName(), "dummyKMeans"));
    // }
  }

}
