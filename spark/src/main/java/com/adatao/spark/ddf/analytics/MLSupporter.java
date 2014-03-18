package com.adatao.spark.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.IModel;
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


  @Override
  public DDF applyModel(IModel model) throws DDFException {
    SparkDDF ddf = (SparkDDF) this.getDDF();

    JavaRDD<LabeledPoint> data = ddf.getJavaRDD(LabeledPoint.class);
    JavaRDD result = data.mapPartitions(new applyModelMapper(model));

    String columns = "label double, prediction double";
    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getRawModel().getClass().getName(), "YTrueYPredict"), columns);
    return new SparkDDF(this.getManager(), result.rdd(), double[].class, ddf.getManager().getNamespace(),
        schema.getTableName(), schema);

  }


  // if double[] contain YTrue then the first (n - 1) item will be feature vector
  // the last will be YTrue
  public static class applyModelMapper extends FlatMapFunction<Iterator<LabeledPoint>, double[]> {
    private IModel mModel;

    public applyModelMapper(IModel model) throws DDFException {
      this.mModel = model;
    }

    @Override
    public Iterable<double[]> call(Iterator<LabeledPoint> points) throws DDFException {
      List<double[]> results = new ArrayList<double[]>();

      while (points.hasNext()) {
        LabeledPoint point = points.next();
        try {
          double[] ytrueYpred = new double[] {point.label(), this.mModel.predict(point.features())};
          results.add(ytrueYpred);

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getRawModel()
              .getClass().getName()), e);
        }
      }
      return results;
    }
  }
}
