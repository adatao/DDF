package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;
import com.adatao.ddf.IHandleDDFFunctionalGroup;

/**
 * Common execution for all pAnalytics algorithms
 * 
 * @author bhan
 * 
 */
public interface IRunAlgorithms extends IHandleDDFFunctionalGroup {

  /**
   * Set up parameters and data transformation for the algorithm execution
   */
  // public void preprocess();

  /**
   * @param theDDF
   *          The DDF in the appropriate representation to be processed by the algorithm
   * @return the {@link IAlgorithmModel}
   */
  public IAlgorithmOutputModel run(IAlgorithm algorithm);
}