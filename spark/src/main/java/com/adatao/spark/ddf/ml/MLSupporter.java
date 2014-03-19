package com.adatao.spark.ddf.ml;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.IModel;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;
import com.adatao.spark.ddf.SparkDDF;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.actors.threadpool.Arrays;
import scala.reflect.ClassManifest$;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MLSupporter extends com.adatao.ddf.ml.MLSupporter {

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
    return this.applyModel(model, false, false);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels) throws DDFException {
    return this.applyModel(model, hasLabels, false);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    SparkDDF ddf = (SparkDDF) this.getDDF();

    JavaRDD<double[]> data = ddf.getJavaRDD(double[].class);
    JavaRDD<double[]> result = data.mapPartitions(new predictMapper(model, hasLabels, includeFeatures));

    String columns = "label double, prediction double";
    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getRawModel().getClass().getName(),
        "YTrueYPredict"), columns);

    return new SparkDDF(this.getManager(), result.rdd(), double[].class, ddf.getManager().getNamespace(),
        schema.getTableName(), schema);
  }


  // if double[] contain YTrue then the first (n - 1) item will be feature vector
  // the last will be YTrue
  public static class predictMapper extends FlatMapFunction<Iterator<double[]>, double[]> {
    private static final long serialVersionUID = 1L;
    private IModel mModel;
    private boolean mHasLabels;
    private boolean mIncludeFeatures;


    public predictMapper(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
      mModel = model;
      mHasLabels = hasLabels;
      mIncludeFeatures = includeFeatures;
    }

    @Override
    public Iterable<double[]> call(Iterator<double[]> samples) throws DDFException {
      List<double[]> results = new ArrayList<double[]>();

      while (samples.hasNext()) {

        double[] features = samples.next();
        double[] outputRow = null;

        try {
          if (mHasLabels) {
            // label, prediction
            double label = features[features.length - 1];
            features = Arrays.copyOf(features, features.length - 1);
            outputRow = new double[] { label, this.mModel.predict(features) };
          } else {
            // prediction
            outputRow = new double[] { this.mModel.predict(features) };
          }

          if (mIncludeFeatures) {
            // features, (optional label), prediction
            outputRow = ArrayUtils.addAll(features, outputRow);
          }

          results.add(outputRow);

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getRawModel().getClass()
              .getName()), e);
        }
      }
      return results;
    }
  }
}
