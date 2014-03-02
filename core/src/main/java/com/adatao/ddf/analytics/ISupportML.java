package com.adatao.ddf.analytics;


import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 * Interface for handling tasks related to Machine Learning
 */
public interface ISupportML extends IHandleDDFFunctionalGroup {

  /**
   * Trains a model using data from this DDF.
   * 
   * @param algorithm
   * @return
   */
  public IModel run(IAlgorithm algorithm);



  interface IAlgorithm {
    IHyperParameters getHyperParameters();

    void setHyperParameters(IHyperParameters params);

    Class<?> getInputClass();

    void setInputClass(Class<?> inputClass);

    public Object prepare(Object data);

    public IModel run(Object data);
  }

  interface IModel extends IHandlePersistence.IPersistible {
    IModelParameters getParameters();

    void setParameters(IModelParameters parameters);
  }

  interface IHyperParameters {

  }

  interface IModelParameters {

  }

}
