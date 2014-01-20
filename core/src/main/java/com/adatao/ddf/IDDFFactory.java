package com.adatao.ddf;

public interface IDDFFactory {
  /**
   * Instantiates and sets all the supported handlers for the new DDF
   * 
   * @return a new DDF with all the supported handlers set
   */
  public DDF newDDF();
}
