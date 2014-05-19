package com.adatao.ddf.etl;

import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {
  
  public DDF transformScaleMinMax() throws DDFException;

  public DDF transformScaleStandard() throws DDFException;
  
  public DDF transformNativeRserve(String transformExpression);
  
  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);
  
  public DDF transformUDF(String transformExpression, List<String> columns) throws DDFException;

}
