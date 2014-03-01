package com.adatao.ddf.analytics;


import java.io.Serializable;

/**
 * A IAlgorithmOutputModel represents the output result of an algorithm
 * 
 * @author bhan
 * 
 */
public interface IAlgorithmOutputModel extends Serializable {
  /**
   * Export output model to disk
   */
  void persist();

  void unpersist();
}
