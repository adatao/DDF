package com.adatao.spark.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;
import com.adatao.spark.ddf.SparkDDF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifest$;

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

  /*
   * @Override protected <T, U> DDF predictImpl(DDF ddf, Class<T> predictReturnType, Class<U> predictInputType, Object
   * model) throws DDFException { int numCols = ddf.getNumColumns();
   * 
   * RDD rdd = (RDD<U>) ddf.getRepresentationHandler().get(RDD.class, predictInputType);
   * 
   * JavaRDD<U> data = new JavaRDD<U>(rdd, ClassManifest$.MODULE$.fromClass(predictInputType));
   * 
   * String columnName = this.getColumnName(predictReturnType);
   * 
   * JavaRDD result = data.mapPartitions(new partitionMapper<T, U>(model)); Schema schema = new
   * Schema(String.format("%s_%s_%s", ddf.getName(), model.getClass().getName(), "prediction"),
   * String.format("prediction %s", columnName));
   * 
   * return new SparkDDF(this.getManager(), result.rdd(), predictReturnType, ddf.getManager().getNamespace(),
   * schema.getTableName(), schema); }
   */

  private String getColumnName(Class<?> clazz) {
    if (clazz == Double.class) return "double";
    else if (clazz == Integer.class) return "int";
    else return clazz.getName();
  }

  @Override
  public DDF getYTrueYPredImpl(IModel model) throws DDFException {
    model = new Model(model.getInternalModel());
    DDF ddf = this.getDDF();
    RDD rdd = (RDD<LabeledPoint>) ddf.getRepresentationHandler().get(RDD.class, LabeledPoint.class);

    JavaRDD<LabeledPoint> data = new JavaRDD<LabeledPoint>(rdd, ClassManifest$.MODULE$.fromClass(LabeledPoint.class));
    JavaRDD result = data.mapPartitions(new ytrueYpredPartitionMapper(model));

    String columnName = this.getColumnName(Double.class);
    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getClass().getName(), "prediction"),
        String.format("prediction %s", columnName));
    return new SparkDDF(this.getManager(), result.rdd(), double[].class, ddf.getManager().getNamespace(),
        schema.getTableName(), schema);
  }

  @Override
  public DDF predictImpl(IModel model) throws DDFException {
    model = new Model(model.getInternalModel());
    DDF ddf = this.getDDF();
    RDD rdd = (RDD<double[]>) ddf.getRepresentationHandler().get(RDD.class, double[].class);

    JavaRDD<double[]> data = new JavaRDD<double[]>(rdd, ClassManifest$.MODULE$.fromClass(double[].class));

    String columnName = this.getColumnName(Double.class);
    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getInternalModel().getClass().getName(),
        "prediction"), String.format("prediction %s", columnName));
    JavaRDD result = data.mapPartitions(new ypredPartitionMapper(model));

    return new SparkDDF(this.getManager(), result.rdd(), Double.class, ddf.getManager().getNamespace(),
        schema.getTableName(), schema);
  }


  // if double[] contain YTrue then the first (n - 1) item will be feature vector
  // the last will be YTrue
  public static class ytrueYpredPartitionMapper extends FlatMapFunction<Iterator<LabeledPoint>, double[]> {
    private IModel mModel;

    public ytrueYpredPartitionMapper(IModel model) throws DDFException {
      this.mModel = model;
    }

    @Override
    public Iterable<double[]> call(Iterator<LabeledPoint> points) throws DDFException {
      List<double[]> results = new ArrayList<double[]>();

      while (points.hasNext()) {
        LabeledPoint point = points.next();
        try {
          double[] ytrueYpred = new double[] { point.label(), this.mModel.predict(point.features()) };
          results.add(ytrueYpred);

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getInternalModel()
              .getClass().getName()), e);
        }
      }
      return results;
    }
  }

  public static class ypredPartitionMapper extends FlatMapFunction<Iterator<double[]>, Double> {
    private IModel mModel;

    public ypredPartitionMapper(IModel model) throws DDFException {
      this.mModel = model;
    }

    @Override
    public Iterable<Double> call(Iterator<double[]> points) throws DDFException {
      List<Double> results = new ArrayList<Double>();

      while (points.hasNext()) {
        double[] point = points.next();
        try {
          results.add(this.mModel.predict(point));
        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getInternalModel()
              .getClass().getName()), e);
        }
      }
      return results;
    }
  }
}
