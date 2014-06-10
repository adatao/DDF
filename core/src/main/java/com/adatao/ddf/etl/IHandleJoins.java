package com.adatao.ddf.etl;

import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDF.JoinType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleJoins extends IHandleDDFFunctionalGroup {

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns, List<String> byRightColumns) throws DDFException;
}
