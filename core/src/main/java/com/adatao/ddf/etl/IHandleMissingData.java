package com.adatao.ddf.etl;

import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMissingData extends IHandleDDFFunctionalGroup {

  public DDF dropNA(int axis, String any, long thresh, List<String> subset, boolean inplace) throws DDFException;
  public DDF fillNA();
  public DDF replaceNA(); 
  
}
