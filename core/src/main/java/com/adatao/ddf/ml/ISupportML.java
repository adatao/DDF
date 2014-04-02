package com.adatao.ddf.ml;


import com.adatao.ddf.DDF;
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
  IModel train(String trainMethodName, Object... args) throws DDFException;

  /**
   * The DDF is assumed not to have a label column, and the output should not include the feature columns.
   * 
   * @param model
   * @return
   * @throws DDFException
   */
  DDF applyModel(IModel model) throws DDFException;

  /**
   * The output by default will not include the feature columns.
   * 
   * @param model
   * @param hasLabels
   * @return
   * @throws DDFException
   */
  DDF applyModel(IModel model, boolean hasLabels) throws DDFException;

  /**
   * 
   * @param model
   * @param hasLabels
   *          if true, then the DDF does include a label column
   * @param includeFeatures
   *          if true, then the output should include all the feature columns
   * @return
   * @throws DDFException
   */
  DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException;

  /*
   * Compute the binary confusion matrix of this DDF based on the given model.
  */
  Long[][] getConfusionMatrix(IModel model, double threshold) throws DDFException;
}
