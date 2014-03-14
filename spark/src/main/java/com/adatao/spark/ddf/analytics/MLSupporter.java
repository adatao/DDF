package com.adatao.spark.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;
import com.adatao.spark.ddf.SparkDDF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifest$;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
  protected Object convertDDF(ParamInfo paramInfo) throws DDFException {
    if (paramInfo.argMatches(RDD.class)) {
      // Yay, our target data format is an RDD!
      RDD<?> rdd = null;

      if (paramInfo.paramMatches(LabeledPoint.class)) {
        rdd = (RDD<LabeledPoint>) this.getDDF().getRepresentationHandler().get(RDD.class, LabeledPoint.class);
        System.out.println("RDD<LabeledPoint>");
      } else if (paramInfo.paramMatches(double[].class)) {
        rdd = (RDD<double[]>) this.getDDF().getRepresentationHandler().get(RDD.class, double[].class);
        System.out.println("RDD<Double[]>");
      } else if (paramInfo.paramMatches(Object.class)) {
        rdd = (RDD<Object[]>) this.getDDF().getRepresentationHandler().get(RDD.class, Object[].class);
        System.out.println("RDD<Object>");
      }
      return rdd;
    }

    else {
      return super.convertDDF(paramInfo);
    }
  }

  public DDF predict(Object model) throws DDFException {
    DDF ddf = this.getDDF();
    Method predictMethod = null;
    RDD rdd = (RDD<double[]>) ddf.getRepresentationHandler().get(RDD.class, double[].class);
    JavaRDD<double[]> data = new JavaRDD<double[]>(rdd, ClassManifest$.MODULE$.fromClass(double[].class));

    try {
      predictMethod = model.getClass().getMethod("predict", new Class[] { double[].class });
    } catch (Exception e) {
      throw new DDFException(e);
    }
    Class<?> returnType = predictMethod.getReturnType();
    String modelName = model.getClass().getName();

    JavaRDD result = null;

    if(returnType == double.class || returnType == Double.class){
      result = data.mapPartitions(new partitionMapper<Double>(model));
      Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), modelName, "prediction"), "clusterID double");

      return new SparkDDF(this.getManager(), result.rdd(), Double.class, ddf.getManager().getNamespace(),
          schema.getTableName(), schema);

    } else if(returnType == int.class || returnType == Integer.class ) {

      result = data.mapPartitions(new partitionMapper<Integer>(model));
      Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), modelName, "prediction"), "clusterID int");

      return new SparkDDF(this.getManager(), result.rdd(), Integer.class, ddf.getManager().getNamespace(),
          schema.getTableName(), schema);
    } else {
      throw new DDFException("error ");
    }

  }

  public static class partitionMapper<T> extends FlatMapFunction<Iterator<double[]>, T> {
    private Object mModel;

    private Method getPredictMethod() throws DDFException{
      Method predictMethod = null;
      try {
        predictMethod = mModel.getClass().getMethod("predict", new Class[] { double[].class });
      } catch (Exception e) {
        throw new DDFException(e);
      }
      return predictMethod;
    }

    public partitionMapper(Object model) throws DDFException{
      this.mModel = model;
    }

    @Override
    public Iterable<T> call(Iterator<double[]> points) throws DDFException {
      Method predictMethod = this.getPredictMethod();
      if(predictMethod == null) {
        throw new DDFException("");
      }
      List<T> results = new ArrayList<T>();

      while(points.hasNext()){
        double[] point = points.next();
        try {
            results.add((T) predictMethod.invoke(this.mModel, point));

        } catch (Exception e) {
          if(e instanceof DDFException) throw new DDFException(e);
          else throw new DDFException(e);
        }
      }
      return results;
    }
  }
}
