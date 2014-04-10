package com.adatao.spark.ddf.ml;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandleRepresentations.IGetResult;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.IModel;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;
import com.adatao.spark.ddf.SparkDDF;
import com.adatao.spark.ddf.analytics.CrossValidation;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.actors.threadpool.Arrays;

import com.adatao.ddf.types.TupleMatrixVector;


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
      

      System.out.println(">>>>>>>>>>>>>... spark MLSupporter convertDDF : paramInfo = " + paramInfo);
      
      if (paramInfo.paramMatches(LabeledPoint.class)) {
        rdd = (RDD<LabeledPoint>) this.getDDF().getRepresentationHandler().get(RDD.class, LabeledPoint.class);
        // System.out.println("RDD<LabeledPoint>");

      } else if (paramInfo.paramMatches(double[].class)) {
        rdd = (RDD<double[]>) this.getDDF().getRepresentationHandler().get(RDD.class, double[].class);
        System.out.println("RDD<Double[]>");
      } 
      else if (paramInfo.paramMatches(TupleMatrixVector.class)) {
        System.out.println(">>>>>>>>>>>>>... insideconvertDDF : paramInfo = " + paramInfo);
        rdd = (RDD<TupleMatrixVector>) this.getDDF().getRepresentationHandler().get(RDD.class, TupleMatrixVector.class);
        
        System.out.println(">>>>>>>>>>>>>... finish parsing Matrix Vector");
        System.out.println("RDD<TupleMatrixVector>");
      } 
      else if (paramInfo.paramMatches(Object.class)) {
        rdd = (RDD<Object[]>) this.getDDF().getRepresentationHandler().get(RDD.class, Object[].class);
        // System.out.println("RDD<Object>");
      }
      return rdd;
    }

    else {
      System.out.println("paramInfo >>>>>>>>>" +paramInfo);
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

  @SuppressWarnings("unchecked")
  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    SparkDDF ddf = (SparkDDF) this.getDDF();
    IGetResult gr = ddf.getJavaRDD(double[].class, LabeledPoint.class, Object[].class);

    // Apply appropriate mapper
    JavaRDD<?> result = null;
    Class<?> resultUnitType = double[].class;

    if (LabeledPoint.class.equals(gr.getTypeSpecs()[0])) {
      result = ((JavaRDD<LabeledPoint>) gr.getObject()).mapPartitions(new PredictMapper<LabeledPoint, double[]>(
          LabeledPoint.class, double[].class, model, hasLabels, includeFeatures));

    } else if (double[].class.equals(gr.getTypeSpecs()[0])) {
      result = ((JavaRDD<double[]>) gr.getObject()).mapPartitions(new PredictMapper<double[], double[]>(double[].class,
          double[].class, model, hasLabels, includeFeatures));

    } else if (Object[].class.equals(gr.getTypeSpecs()[0])) {
      result = ((JavaRDD<Object[]>) gr.getObject()).mapPartitions(new PredictMapper<Object[], Object[]>(Object[].class,
          Object[].class, model, hasLabels, includeFeatures));
      resultUnitType = Object[].class;
    } else {
      throw new DDFException(String.format("Error apply model %s", model.getRawModel().getClass().getName()));
    }


    // Build schema
    List<Schema.Column> outputColumns = new ArrayList<Schema.Column>();

    if (includeFeatures) {
      outputColumns = ddf.getSchema().getColumns();

    } else if (!includeFeatures && hasLabels) {
      outputColumns.add(ddf.getSchema().getColumns().get(ddf.getNumColumns() - 1));
    }

    outputColumns.add(new Schema.Column("prediction", "double"));

    Schema schema = new Schema(String.format("%s_%s_%s", ddf.getName(), model.getRawModel().getClass().getName(),
        "YTrueYPredict"), outputColumns);


    if (double[].class.equals(resultUnitType)) {
      return new SparkDDF(this.getManager(), (RDD<double[]>) result.rdd(), double[].class, ddf.getManager()
          .getNamespace(), schema.getTableName(), schema);

    } else if (Object[].class.equals(resultUnitType)) {
      return new SparkDDF(this.getManager(), (RDD<Object[]>) result.rdd(), Object[].class, ddf.getManager()
          .getNamespace(), schema.getTableName(), schema);

    } else return null;
  }


  private static class PredictMapper<I, O> extends FlatMapFunction<Iterator<I>, O> {

    private static final long serialVersionUID = 1L;
    private IModel mModel;
    private boolean mHasLabels;
    private boolean mIncludeFeatures;
    private Class<?> mInputType;
    private Class<?> mOutputType;


    public PredictMapper(Class<I> inputType, Class<O> outputType, IModel model, boolean hasLabels,
        boolean includeFeatures) throws DDFException {

      mInputType = inputType;
      mOutputType = outputType;
      mModel = model;
      mHasLabels = hasLabels;
      mIncludeFeatures = includeFeatures;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<O> call(Iterator<I> samples) throws DDFException {
      List<O> results = new ArrayList<O>();

      System.out.println(">>>>>>>>>>>>>>applyModel: calling method");
      
      while (samples.hasNext()) {
    	  
    	  System.out.println(">>>>>>>>>>>>>>applyModel: reading sample");

        I sample = samples.next();
        O outputRow = null;

        try {
          if (sample instanceof LabeledPoint || sample instanceof double[]) {
        	  
        	  System.out.println(">>>>>>>>>>>>>>applyModel: reading sample double[]");
        	  
            double label = 0;
            double[] features;

            if (sample instanceof LabeledPoint) {
              LabeledPoint s = (LabeledPoint) sample;
              label = s.label();
              features = s.features();

            } else {
              double[] s = (double[]) sample;
              if (mHasLabels) {
                label = s[s.length - 1];
                features = Arrays.copyOf(s, s.length - 1);
              } else {
                features = s;
              }
            }


            if (double[].class.equals(mOutputType)) {
              if (mHasLabels) {
                outputRow = (O) new double[] { label, (Double) this.mModel.predict(features) };
              } else {
                outputRow = (O) new double[] { (Double) this.mModel.predict(features) };
              }

              System.out.println(">>>>>>>>>>>>>>applyModel:  after predict double[]");
              if (mIncludeFeatures) {
                outputRow = (O) ArrayUtils.addAll(features, (double[]) outputRow);
              }

            } else if (Object[].class.equals(mOutputType)) {
              if (mHasLabels) {
                outputRow = (O) new Object[] { label, this.mModel.predict(features) };
              } else {
                outputRow = (O) new Object[] { this.mModel.predict(features) };
              }

              if (mIncludeFeatures) {
                Object[] oFeatures = new Object[features.length];
                for (int i = 0; i < features.length; i++) {
                  oFeatures[i] = (Object) features[i];
                }
                outputRow = (O) ArrayUtils.addAll(oFeatures, (Object[]) outputRow);
              }

            } else {
              throw new DDFException(String.format("Unsupported output type %s", mOutputType));
            }


          } else if (sample instanceof Object[]) {
            Object label = null;
            Object[] features;

            Object[] s = (Object[]) sample;
            if (mHasLabels) {
              label = s[s.length - 1];
              features = Arrays.copyOf(s, s.length - 1);
            } else {
              features = s;
            }

            double[] dFeatures = new double[features.length];
            for (int i = 0; i < features.length; i++) {
              dFeatures[i] = (Double) features[i];
            }

            if (mHasLabels) {
              outputRow = (O) new Object[] { label, this.mModel.predict(dFeatures) };
            } else {
              outputRow = (O) new Object[] { this.mModel.predict(dFeatures) };
            }

            if (mIncludeFeatures) {
              outputRow = (O) ArrayUtils.addAll(features, (Object[]) outputRow);
            }

          } else {
            throw new DDFException(String.format("Unsupported input type %s", mInputType));
          }


          System.out.println(">>>>>>>>>>>>>>outputRow");
          results.add(outputRow);

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getRawModel().getClass()
              .getName()), e);
        }
      }

      return results;
    }
  }


  public List<List<DDF>> CVKFold(int k, Long seed) throws DDFException {
    return CrossValidation.DDFKFoldSplit(this.getDDF(), k, seed);
  }

  public List<List<DDF>> CVRandom(int k, double trainingSize, Long seed) throws DDFException {
    return CrossValidation.DDFRandomSplit(this.getDDF(), k, trainingSize, seed);
  }
}
