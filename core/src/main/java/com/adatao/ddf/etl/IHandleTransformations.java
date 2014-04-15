package com.adatao.ddf.etl;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {
  
  public DDF transformScaleMinMax() throws DDFException;

  public DDF transformScaleStandard() throws DDFException;
  
  public DDF transformNativeRserve(String transformExpression);


}
