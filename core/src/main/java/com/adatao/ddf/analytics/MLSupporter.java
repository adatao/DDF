package com.adatao.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri;
import com.adatao.ddf.content.IHandlePersistence.IPersistible;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.local.ddf.LocalObjectDDF;
import com.google.gson.annotations.Expose;

/**
 */
public class MLSupporter extends ADDFFunctionalGroupHandler implements ISupportML {

  public MLSupporter(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public IModel run(IAlgorithm algorithm) {
    if (algorithm == null) return null;

    Object data = this.getDDF().getRepresentationHandler().get(algorithm.getInputClass());
    Object preparedData = algorithm.prepare(data);
    return algorithm.run(preparedData);
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
    public IModel run(Object data) {
      return null;
    }
  }

  public static class Model implements IModel, IPersistible {

    public static final long serialVersionUID = 1L;
    private DDF mDDF;
    @Expose public final long mSerialVersionUID = serialVersionUID;

    private IModelParameters mParams;


    @Override
    public IModelParameters getParameters() {
      return mParams;
    }

    @Override
    public void setParameters(IModelParameters parameters) {
      mParams = parameters;
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
}
