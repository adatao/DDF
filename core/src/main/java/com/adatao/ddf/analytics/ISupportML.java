package com.adatao.ddf.analytics;


import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 * Interface for handling tasks related to Machine Learning
 */
public interface ISupportML extends IHandleDDFFunctionalGroup {

  /**
   * Runs an unsupervised (unlabled) training algorithm on the entire DDF dataset.
   * 
   * @param trainMethodName
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, Object... args) throws DDFException;

  /**
   * Trains a model, given an trainMethod name or className#methodName (e.g., "kmeans" or
   * "org.apache.spark.mllib.kmeans#train"). If #methodName is not specified, it is assumed to be "train"
   * 
   * @param trainMethod
   * @param featureColumnIndexes
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, int[] featureColumnIndexes, Object... args) throws DDFException;

  /**
   * @param trainMethod
   * @param featureColumnIndexes
   * @param targetColumnIndex
   * @param args
   * 
   */
  public IModel train(String trainMethodName, int targetColumnIndex, int[] featureColumnIndexes, Object... args)
      throws DDFException;

  /**
   * Trains a model, given an trainMethod name or className#methodName (e.g., "kmeans" or
   * "org.apache.spark.mllib.kmeans#train"). If #methodName is not specified, it is assumed to be "train"
   * 
   * @param trainMethod
   * @param featureColumnNames
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, String[] featureColumnNames, Object... args) throws DDFException;

  /**
   * @param trainMethod
   * @param featureColumnNames
   * @param targetColumnIndex
   * @param args
   * 
   */
  public IModel train(String trainMethodName, String targetColumnName, String[] featureColumnNames, Object... args)
      throws DDFException;



  /**
   * 
   */
  interface IAlgorithm {
    IHyperParameters getHyperParameters();

    void setHyperParameters(IHyperParameters params);

    Class<?> getInputClass();

    void setInputClass(Class<?> inputClass);

    public Object prepare(Object data);

    public IModel run(Object data);
  }



  /**
   *
   */
  interface IModel extends IHandlePersistence.IPersistible {
    IModelParameters getParameters();

    void setParameters(IModelParameters parameters);
  }



  /**
   * 
   */
  interface IHyperParameters {}



  /**
   * 
   */
  interface IModelParameters {}

}
