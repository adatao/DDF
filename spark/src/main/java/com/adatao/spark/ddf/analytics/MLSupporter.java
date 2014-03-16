package com.adatao.spark.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.MLPredictMethod;
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
import scala.actors.threadpool.Arrays;
import scala.reflect.ClassManifest$;

import java.lang.reflect.InvocationTargetException;
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


  @Override
  protected  <T, U> DDF predictImpl(DDF ddf, Class<T> predictReturnType, Class<U> predictInputType, Object model) throws DDFException {
    int numCols = ddf.getNumColumns();

    RDD rdd = (RDD<U>) ddf.getRepresentationHandler().get(RDD.class, predictInputType);

    JavaRDD<U> data = new JavaRDD<U>(rdd, ClassManifest$.MODULE$.fromClass(predictInputType));

    String columnName = this.getColumnName(predictReturnType);

    JavaRDD result = data.mapPartitions(new partitionMapper<T, U>(model));
    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getClass().getName(), "prediction"),
        String.format("prediction %s", columnName));

    return new SparkDDF(this.getManager(), result.rdd(), predictReturnType, ddf.getManager().getNamespace(), schema.getTableName(), schema);
  }

  private String getColumnName(Class<?> clazz) {
    if(clazz == Double.class) return "double";
    else if(clazz == Integer.class) return "int";
    else return clazz.getName();
  }

  public static class partitionMapper<T, U> extends FlatMapFunction<Iterator<U>, T> {
    private Object mModel;

    public partitionMapper(Object model) throws DDFException{
      this.mModel = model;
    }

    @Override
    public Iterable<T> call(Iterator<U> points) throws DDFException {
      // Have to initialize MLPredictMethod here because
      // java.lang.reflect.Method is not serializable
      MLPredictMethod mlPredictMethod = new MLPredictMethod(mModel);
      List<T> results = new ArrayList<T>();

      while(points.hasNext()){
        U point = points.next();
        try {
           results.add((T) mlPredictMethod.getMethod().invoke(this.mModel, point));

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with method %s",
              mlPredictMethod.getMethod().getName()),e);
        }
      }
      return results;
    }
  }
}
