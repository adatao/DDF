package com.adatao.ddf.ml;


/**
 */

import com.adatao.ddf.exception.DDFException;

public interface IModel {

  public Object predict(double[] features) throws DDFException;

  public Object getRawModel();
  
  public String getName();

  public void setName(String name);

}
