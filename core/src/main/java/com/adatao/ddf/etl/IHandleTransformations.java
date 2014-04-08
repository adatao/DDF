package com.adatao.ddf.etl;

import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {
  public void transformNativeRserve(String transformExpression);

}
