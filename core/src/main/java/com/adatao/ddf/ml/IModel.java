package com.adatao.ddf.ml;


/**
 */

import com.adatao.ddf.exception.DDFException;

public interface IModel {

  public Double predict(double[] point) throws DDFException;

  public Object getRawModel();
}
