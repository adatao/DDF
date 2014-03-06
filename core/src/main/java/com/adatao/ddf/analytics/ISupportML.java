package com.adatao.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

/**
 * Interface for handling tasks related to Machine Learning
 */
public interface ISupportML extends IHandleDDFFunctionalGroup {

  /**
   * Trains a model using data from this DDF.
   * 
   * @param algorithm
   * @return
   * @throws DDFException
   */
  public IModel train(IAlgorithm algorithm, Object... params) throws DDFException;

  /**
   * Trains a model, given an algorithm name or className#methodName (e.g., "kmeans" or
   * "org.apache.spark.mllib.kmeans#train")
   * 
   * @param algorithm
   * @param params
   * @return
   * @throws DDFException
   */
  public IModel train(String algorithm, Object... params) throws DDFException;


  interface IAlgorithm {
    IHyperParameters getHyperParameters();

    void setHyperParameters(IHyperParameters params);

    Class<?> getInputClass();

    void setInputClass(Class<?> inputClass);

    public Object prepare(Object data);

    public IModel run(Object data, List<String> featureColumnNames);
  }

  interface IModel extends IHandlePersistence.IPersistible {
    IHyperParameters getParameters();

    void setParameters(IHyperParameters parameters);

    public DDF predict(DDF ddf) throws DDFException;
  }

  interface IHyperParameters {

  }

  interface IModelParameters {

  }

}
