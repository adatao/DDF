package com.adatao.ddf;

public interface IDataFrameFactory {
  /**
   * Instantiates and sets all the supported handlers for the new DataFrame
   * 
   * @return a new DataFrame with all the supported handlers set
   */
  public DDF newDataFrame();
}
