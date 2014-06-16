package com.adatao.ddf.ml;


/**
 */

import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.types.TJsonSerializable;

public interface IModel {

  public Object predict(double[] features) throws DDFException;

  public Object getRawModel();

  public void setRawModel(Object rawModel);

  public String getName();

  public void setName(String name);

  public DDF serialize2DDF(DDFManager manager) throws DDFException;

  public String toJson();
}
