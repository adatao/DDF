package com.adatao.ddf.facades;


import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils;
import java.util.ArrayList;
import java.util.List;

/**
 */

public class PAFacade {

  private DDF mDDF;

  public PAFacade(DDF ddf) {
    mDDF = ddf;
  }

  public Object runCommand(String methodName, Object... params) throws DDFException {
    List<Class<?>> argTypes = new ArrayList<Class<?>>();

    if (params != null && params.length > 0) {
      for (Object param : params) {
        argTypes.add(param == null ? Object.class : param.getClass());
      }
    }
    Utils.ClassMethod method = new Utils.ClassMethod(this, methodName, argTypes.toArray(new Class<?>[0]));

    try {
      return method.instanceInvoke(params);
    } catch (DDFException e) {
      throw new DDFException("Error invoking method " + methodName);
    }
  }

  public Object nrow() throws DDFException {
    return mDDF.getNumRows();
  }

  public Object getNumColumns() {
    return mDDF.getNumColumns();
  }

  public Object summary() throws DDFException {
    return mDDF.getSummary();
  }
}
