package com.adatao.ddf.etl;


import java.util.List;
import java.util.Map;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;
import com.adatao.ddf.types.AggregateTypes.AggregateFunction;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

public interface IHandleMissingData extends IHandleDDFFunctionalGroup {

  public DDF dropNA(Axis axis, NAChecking how, long thresh, List<String> columns) throws DDFException;

  public DDF fillNA(String value, FillMethod method, long limit, AggregateFunction function,
      Map<String, String> columnsToValues, List<String> columns) throws DDFException;

  public enum Axis {
    @SerializedName("row") ROW, @SerializedName("column") COLUMN;

  }

  public enum NAChecking {
    @SerializedName("any") ANY, @SerializedName("all") ALL;
  }

  public enum FillMethod {
    @SerializedName("bfill") BFILL, @SerializedName("ffill") FFILL;

    public static FillMethod fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (FillMethod t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }


}
