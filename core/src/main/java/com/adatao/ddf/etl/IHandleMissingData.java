package com.adatao.ddf.etl;

import java.util.List;
import java.util.Map;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMissingData extends IHandleDDFFunctionalGroup {

  public DDF dropNA(int axis, String any, long thresh, List<String> subset, boolean inplace) throws DDFException;
  public DDF fillNA(String value, String method, long limit, String function, Map<String, String> columnsToValues, List<String> columns, boolean inplace) throws DDFException;
  public DDF replaceNA(); 
  
}
