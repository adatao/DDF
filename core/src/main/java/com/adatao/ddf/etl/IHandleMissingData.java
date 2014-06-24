package com.adatao.ddf.etl;


import java.util.List;
import java.util.Map;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;
import com.adatao.ddf.types.AggregateTypes.AggregateFunction;
import com.google.common.base.Strings;

public interface IHandleMissingData extends IHandleDDFFunctionalGroup {

  public DDF dropNA(Axis axis, NAChecking how, long thresh, List<String> columns) throws DDFException;

  public DDF fillNA(String value, FillMethod method, long limit, AggregateFunction function,
      Map<String, String> columnsToValues, List<String> columns) throws DDFException;

  public DDF replaceNA();


  public enum Axis {
    ROW, COLUMN;

  }

  public enum NAChecking {
    ANY, ALL;
  }

  public enum FillMethod {
    BFILL, FFILL;

    public static FillMethod fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (FillMethod t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }


}
