package com.adatao.ddf.analytics;


import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 * Interface for handling tasks related to Machine Learning
 */
public interface ISupportML extends IHandleDDFFunctionalGroup {

  /**
   * Trains a model using data and featureColumns as features from this DDF.
   * 
   * @param algorithm
   * @param featureColumnIndexes
   * @return
   * @throws DDFException
   */
  public IModel train(IAlgorithm algorithm, int[] featureColumnIndexes, Object... params) throws DDFException;

  /**
   *
   */
  public IModel train(IAlgorithm algorithm, int[] featureColumnIndexes, int targetColumnIndex, Object... params)
      throws DDFException;

  /**
   * Trains a model, given an algorithm name or className#methodName (e.g., "kmeans" or
   * "org.apache.spark.mllib.kmeans#train")
   * 
   * @param algorithm
   * @param featureColumnIndexes
   * @param params
   * @return
   * @throws DDFException
   */
  public IModel train(String algorithm, int[] featureColumnIndexes, Object... params) throws DDFException;

  /**
   * @param algorithm
   * @param featureColumnIndexes
   * @param targetColumnIndex
   * @param params
   * 
   */
  public IModel train(String algorithm, int[] featureColumnIndexes, int targetColumnIndex, Object... params)
      throws DDFException;


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
