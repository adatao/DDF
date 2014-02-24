package com.adatao.ddf;



public interface IHandleDDFFunctionalGroup {

  /**
   * @return the {@link DDF} this handler handles, via the DDFManager
   */
  public abstract DDF getDDF();


  /**
   * Sets the DDF that we are a handler for
   * 
   * @param theDDF
   */
  public abstract void setDDF(DDF theDDF);
}