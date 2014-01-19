package com.adatao.DDF;

public interface IDataFrameFactory {
  /**
   * Instantiates and sets all the supported handlers for the new DataFrame
   * 
   * @return a new DataFrame with all the supported handlers set
   */
  public DataFrame newDataFrame();
}
