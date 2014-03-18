package com.adatao.ddf.analytics;


import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;
import com.adatao.ddf.ml.IModel;

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

  public DDF applyModel(IModel model) throws DDFException;
}
