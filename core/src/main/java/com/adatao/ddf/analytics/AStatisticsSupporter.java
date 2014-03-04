package com.adatao.ddf.analytics;

import java.io.Serializable;
import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.ColumnType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.google.common.base.Strings;

/**
 * 
 * @author bhan
 * 
 */
public abstract class AStatisticsSupporter extends ADDFFunctionalGroupHandler implements ISupportStatistics {

  public AStatisticsSupporter(DDF theDDF) {
    super(theDDF);
  }

  private Summary[] basicStats;

  protected abstract Summary[] getSummaryImpl();

  public Summary[] getSummary() {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }


  @Override
  public FiveNumSummary getColumnFiveNumSummary(String columnName) throws DDFException {
    FiveNumSummary fivenum = new FiveNumSummary();
    ColumnType colType = this.getDDF().getColumn(columnName).getType();
    String cmdInt = String.format("SELECT PERCENTILE(%s, array(0, 1, 0.25, 0.5, 0.75)) FROM %%s", columnName);
    String cmdDouble = String.format("SELECT MIN(%s), MAX(%s), PERCENTILE_APPROX(%s, array(0.25, 0.5, 0.75)) FROM %%s",
        columnName, columnName, columnName);
    String cmd = "";
    switch (colType) {
      case INT:
        cmd = cmdInt;
      case LONG:
        cmd = cmdInt;
      case DOUBLE:
        cmd = cmdDouble;
      case FLOAT:
        cmd = cmdDouble;
      default:
        fivenum = new FiveNumSummary(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }
    if (!Strings.isNullOrEmpty(cmd)) {
      // result for cmdInt is in the format "[min, max, 1st_quantile, median, 3rd_quantile]" where each value can be a NULL
      // result for cmdDouble is in the format "min \t max \t[1st_quantile, median, 3rd_quantile]" or "min \t max \t null"
      String[] rs = this.getDDF().runSql2txt(cmd, String.format("Unable to fetch %s rows from table %%s", columnName))
          .get(0).replace("[", "").replace("]", "").replaceAll("\t", ",").replace("null", "NULL, NULL, NULL").split(",");
      Double[] values = new Double[rs.length];
      for (int i = 0; i < rs.length; i++) {
        values[i] = Double.parseDouble(rs[i]);
      }

      fivenum = new FiveNumSummary(values[0], values[1], values[2], values[3], values[4]);
    }
    return fivenum;
  }

  @Override
  public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException {
    FiveNumSummary[] fivenums = new FiveNumSummary[columnNames.size()];
    for (int i=0; i<columnNames.size(); i++) {
      fivenums[i] = getColumnFiveNumSummary(columnNames.get(i));
    }
    return fivenums;
  }
  
  public static class FiveNumSummary implements Serializable {

    private static final long serialVersionUID = 1L;
    private double mMin = 0;
    private double mMax = 0;
    private double mFirst_quantile = 0;
    private double mMedian = 0;
    private double mThird_quantile = 0;

    public FiveNumSummary() {
      
    }
    public FiveNumSummary(double mMin, double mMax, double first_quantile, double median, double third_quantile) {
      this.mMin = mMin;
      this.mMax = mMax;
      this.mFirst_quantile = first_quantile;
      this.mMedian = median;
      this.mThird_quantile = third_quantile;
    }
    public double getMin() {
      return mMin;
    }
    public void setMin(double mMin) {
      this.mMin = mMin;
    }
    public double getMax() {
      return mMax;
    }
    public void setMax(double mMax) {
      this.mMax = mMax;
    }
    public double getFirst_quantile() {
      return mFirst_quantile;
    }
    public void setFirst_quantile(double mFirst_quantile) {
      this.mFirst_quantile = mFirst_quantile;
    }
    public double getMedian() {
      return mMedian;
    }
    public void setMedian(double mMedian) {
      this.mMedian = mMedian;
    }
    public double getThird_quantile() {
      return mThird_quantile;
    }
    public void setThird_quantile(double mThird_quantile) {
      this.mThird_quantile = mThird_quantile;
    }

  }
}
