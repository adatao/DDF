package com.adatao.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri;
import com.adatao.ddf.content.IHandlePersistence.IPersistible;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.misc.Config.ConfigConstant;
import com.adatao.ddf.util.Utils.ClassMethod;
import com.adatao.local.ddf.LocalObjectDDF;
import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;

import java.util.List;

/**
 */
public class MLSupporter extends ADDFFunctionalGroupHandler implements ISupportML {

  private Boolean sIsNonceInitialized = false;


  /**
   * For ML reflection test-case only
   */
  @Deprecated
  public MLSupporter() {
    super(null);
  }

  public MLSupporter(DDF theDDF) {
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

  private void initializeConfiguration() {
    if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans"))) {
      Config.set(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans",
          String.format("%s#%s", MLSupporter.class.getName(), "dummyKMeans"));
    }
  }

  @Override
  public IModel train(IAlgorithm algorithm, Object... parameters) {
    if (algorithm == null) return null;

    Object data = this.getDDF().getRepresentationHandler().get(algorithm.getInputClass());
    Object preparedData = algorithm.prepare(data);
    return algorithm.run(preparedData, this.getDDF().getColumnNames());
  }

  @Override
  public IModel train(String algorithm, Object... params) throws DDFException {
    if (Strings.isNullOrEmpty(algorithm)) throw new DDFException("Algorithm name cannot be null or empty");

    ClassMethod classMethod = null;

    // First try looking up the specified algorithm in the configuration mapping
    String algoClassHashMethodName = Config.getValueWithGlobalDefault(this.getEngine(), algorithm);
    if (!Strings.isNullOrEmpty(algoClassHashMethodName)) {
      try {
        classMethod = new ClassMethod(algoClassHashMethodName, params);
      } catch (DDFException e) {
        mLog.warn(String.format("Unable to load method %s", algoClassHashMethodName), e);
      }
    }

    // Next, try treating the algorithm string as literally a class#method specification
    if (classMethod == null) classMethod = new ClassMethod(algorithm, params);

    try {
      return (IModel) classMethod.getMethod().invoke(classMethod.getObject(), params);
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }


  public static abstract class AAlgorithm implements IAlgorithm {
    private IHyperParameters mHyperParameters;
    private Class<?> mInputClass;


    public AAlgorithm(Class<?> inputClass, IHyperParameters parameters) {
      this.setInputClass(inputClass);
      this.setHyperParameters(parameters);
    }

    @Override
    public IHyperParameters getHyperParameters() {
      return mHyperParameters;
    }

    @Override
    public void setHyperParameters(IHyperParameters parameters) {
      mHyperParameters = parameters;
    }

    @Override
    public Class<?> getInputClass() {
      return mInputClass;
    }

    @Override
    public void setInputClass(Class<?> inputClass) {
      mInputClass = inputClass;
    }

    @Override
    public Object prepare(Object data) {
      return data;
    }

    @Override
    public IModel run(Object data, List<String> featureColumnNames) {
      return null;
    }
  }

  public abstract static class Model implements IModel, IPersistible {

    public static final long serialVersionUID = 1L;
    private DDF mDDF;
    @Expose public final long mSerialVersionUID = serialVersionUID;

    private IHyperParameters mParams;

    private Class<?> mpredictionInputClass;

    private List<String> mFeatureColumnNames;

    public Model(Class<?> predictionInputClass, IHyperParameters params, List<String> featureColumnNames) {
      mpredictionInputClass = predictionInputClass;
      mParams = params;
      mFeatureColumnNames = featureColumnNames;
    }

    public Class<?> getPredictionInputClass() {
      return mpredictionInputClass;
    }

    @Override
    public IHyperParameters getParameters() {
      return mParams;
    }

    public List<String> getFeatureColumnNames() {
      return mFeatureColumnNames;
    }
    @Override
    public void setParameters(IHyperParameters parameters) {
      mParams = parameters;
    }

    protected Object prepareData(DDF ddf) {
      return ddf.getRepresentationHandler().get(this.getPredictionInputClass());
    }

    protected abstract DDF predictImpl(Object data, String DDFName);

    @Override
    public DDF predict(DDF ddf) throws DDFException {
      List<String> ddfColumnNames = ddf.getColumnNames();

      // if featureColumnNames is not the same as ddf's columnNames
      // project on ddf with fetureColumnNames to get new DDF
      // else perform predict on the provided DDF
      if(!((ddfColumnNames.size() == this.getFeatureColumnNames().size()) &&
          (ddfColumnNames.containsAll(this.getFeatureColumnNames())))){
        DDF newDDF = ddf.Views.project(this.getFeatureColumnNames());
        Object obj = prepareData(newDDF);
        return predictImpl(obj, newDDF.getName());
      } else {
        Object obj = prepareData(ddf);
        return predictImpl(obj, ddf.getName());
      }
    }

    @Override
    public PersistenceUri persist(boolean doOverwrite) throws DDFException {
      mDDF = new LocalObjectDDF<IModel>(this);
      return mDDF.persist();
    }

    @Override
    public PersistenceUri persist() throws DDFException {
      return this.persist(true);
    }

    @Override
    public void unpersist() throws DDFException {
      if (mDDF != null) mDDF.unpersist();
    }

    @Override
    public void beforeSerialization() throws DDFException {}

    @Override
    public void afterDeserialization(Object data) throws DDFException {}

    /**
     * Override to implement additional equality tests
     */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Model)) return false;

      if (this.getParameters() != ((Model) other).getParameters()) return false;

      return true;
    }
  }


  //public IModel dummyKMeans() {
  //  return new Model();
  //}
}
