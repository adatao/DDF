package com.adatao.ddf.analytics;


import com.adatao.ddf.content.IHandlePersistence;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 * Interface for handling tasks related to Machine Learning
 */
public interface ISupportML extends IHandleDDFFunctionalGroup {

  /**
   * Runs a training algorithm on the entire DDF dataset. If the algorithm is unsupervised, all columns are considered
   * to be features. If the algorithm is supervised, the last column is considered to be the target column
   * 
   * @param trainMethodName
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, Object... args) throws DDFException;


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
