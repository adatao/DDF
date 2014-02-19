package com.adatao.ddf.content;

import com.adatao.ddf.DDF;

import java.util.HashMap;
import java.util.List;

/**
 * author: daoduchuan
 */
public abstract class AFactorSupporter implements IFactorSupporter{

  public static class FactorColumnInfo implements AMetaDataHandler.ICustomMetaData {
    /**
     * Column index of the column
     */
    private int mColumnIndex;

    /**
     * List of levels in the column
     */
    private List<String> mLevels;

    /**
     * Mapping from factor to coding for each level
     * E.g. cols = Array["February", "March","April","January"]
     *"February" -> 0 "March" -> 1, "April" -> 2, "January" -> 3
     * The corresponding dummy coding will be
     *            month.f1  month.f2  month.f3
     *  February     0         0         0
     *  Match        1         0         0
     *  April        0         1         0
     *  January      0         0         1
     */
    private HashMap<String, Double> mMap;

    public void setMap(HashMap<String, Double> map) {
      mMap= map;
    }

    public void setColumnIndex(int colIndex) {
      mColumnIndex= colIndex;
    }

    @Override
    public int getColumnIndex() {
      return mColumnIndex;
    }

    public void setLevels(List<String> levels) {
      mLevels= levels;
    }

    public FactorColumnInfo(int columnIndex, List<String> levels, HashMap<String, Double> map) {
      mColumnIndex= columnIndex;
      mMap= map;
      mLevels= levels;
    }

    public FactorColumnInfo(int columnIndex) {
      mColumnIndex= columnIndex;
    }

    public FactorColumnInfo(List<String> levels) {
      mLevels= levels;
    }

    public FactorColumnInfo(HashMap<String, Double> map) {
      mMap= map;
    }

    @Override
    public double[] buildCoding(String value) {

      return null;
    }

    @Override
    public double get(String value, int idx) {

      return 0.0;
    }
  }
}
