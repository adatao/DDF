package com.adatao.ddf.etl;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {
  public DDF transformNativeRserve(String transformExpression);

}
