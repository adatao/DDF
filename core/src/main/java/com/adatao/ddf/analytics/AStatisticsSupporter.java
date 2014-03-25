package com.adatao.ddf.analytics;


import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.ColumnType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

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


  protected abstract Summary[] getSummaryImpl() throws DDFException;



  public Summary[] getSummary() throws DDFException {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }


  @Override
  public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException {
    FiveNumSummary[] fivenums = new FiveNumSummary[columnNames.size()];

    List<String> specs = Lists.newArrayList();
    for (String columnName : columnNames) {
      specs.add(fiveNumFunction(columnName));
    }

    String command = String.format("SELECT %s FROM %%s", StringUtils.join(specs.toArray(new String[0]), ','));

    if (!Strings.isNullOrEmpty(command)) {
      // a fivenumsummary of an Int/Long column is in the format "[min, max, 1st_quantile, median, 3rd_quantile]"
      // each value can be a NULL
      // a fivenumsummary of an Double/Float column is in the format "min \t max \t[1st_quantile, median, 3rd_quantile]"
      // or "min \t max \t null"

      String[] rs = this.getDDF()
          .sql2txt(command, String.format("Unable to get fivenum summary of the given columns from table %%s")).get(0)
          .replace("[", "").replace("]", "").replaceAll("\t", ",").replace("null", "NULL, NULL, NULL").split(",");


      int k = 0;
      for (int i = 0; i < rs.length; i += 5) {
        fivenums[k] = new FiveNumSummary(parseDouble(rs[i]), parseDouble(rs[i + 1]), parseDouble(rs[i + 2]),
            parseDouble(rs[i + 3]), parseDouble(rs[i + 4]));
        k++;
      }

      return fivenums;
    }

    return null;
  }

  private double parseDouble(String s) {
    return ("NULL".equalsIgnoreCase(s)) ? Double.NaN : Double.parseDouble(s);
  }

  private String fiveNumFunction(String columnName) {
    ColumnType colType = this.getDDF().getColumn(columnName).getType();

    switch (colType) {
      case INT:
      case LONG:
        return String.format("PERCENTILE(%s, array(0, 1, 0.25, 0.5, 0.75))", columnName);

      case DOUBLE:
      case FLOAT:
        return String.format("MIN(%s), MAX(%s), PERCENTILE_APPROX(%s, array(0.25, 0.5, 0.75))", columnName, columnName,
            columnName);

      default:
        return "";
    }

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
  
  public Double[] getVectorQuantiles(String columnName, Double[] pArray) throws DDFException {
    return getVectorQuantiles(columnName, pArray, 10000);
  }
  
  public Double[] getVectorQuantiles(String columnName, Double[] pArray, Integer B) throws DDFException {
    if (pArray == null || pArray.length == 0) {
      throw new DDFException("Cannot compute quantiles for empty percenties");
    }
    
    if (Strings.isNullOrEmpty(columnName)) {
      throw new DDFException("Column name must not be empty");
    }
    
    String colType = getDDF().getSchema().getColumn(columnName).getType().name().toLowerCase();
    
    mLog.info("Column type: " + colType);
    List<Double> pValues = Arrays.asList(pArray);
    Pattern p1 = Pattern.compile("^[big|small|tiny]{0,1}int$");
    Pattern p2 = Pattern.compile("^[float|double]$");

    String min = "";
    boolean hasZero = false;
    if (pValues.get(0) == 0) {
      min = "min(" + columnName + ")";
      pValues = pValues.subList(1, pValues.size() - 1);
      hasZero = true;
    }
    
    boolean hasOne = true;
    String max = "";
    if (pValues.get(pValues.size() - 1) == 1.0) {
      max = "max(" + columnName + ")";
      pValues.subList(0, pValues.size() - 1);
      hasOne = true;
    }
    
    String pParams = "";
    
    if (pValues.size() > 0) {
      if (p1.matcher(colType).matches()) {
        pParams = "percentile(" + columnName + ", array(" + StringUtils.join(pArray, ",") + "))";
      } else if (p2.matcher(colType).matches()) {
        pParams = "percentile_approx(" + columnName + ", array(" + StringUtils.join(pArray, ",") + ", " + B.toString() + "))";
      } else {
        throw new DDFException("Only support numeric verctors!!!");
      }
    }
    
    if (min.length() > 0) {
      pParams += ", " + min;
    }
    
    if (max.length() > 0) {
      pParams += ", " + max;
    }
    
    String cmd = "SELECT " + pParams + " FROM " + getDDF().getTableName();
    mLog.info("Command String = " + cmd);
    List<String> rs = getDDF().sql2txt(cmd, "Cannot get vector quantiles from SQL queries");
    if (rs == null || rs.size() ==0) {
      throw new DDFException("Cannot get vector quantiles from SQL queries");
    }
    String[] convertedResults= rs.get(0)
        .replace("[", "").replace("]", "").replaceAll("\t", ",").replace("null", "NULL, NULL, NULL").split(",");
    mLog.info("Raw info " + StringUtils.join(rs, "\n"));
    
    Double[] result = new Double[pArray.length];
    HashMap<Double, Double> mapValues = new HashMap<Double, Double>();
    try {
      for (int i = 0; i < pValues.size(); i++) {
        mapValues.put(pValues.get(i), Double.parseDouble(convertedResults[i]));
      }
      if (hasZero) {
        mapValues.put(0.0, Double.parseDouble(convertedResults[convertedResults.length - 2]));
      }
      if (hasOne) {
        mapValues.put(1.0, Double.parseDouble(convertedResults[convertedResults.length - 1]));
      }
    } catch (NumberFormatException nfe) {
      throw new DDFException("Cannot parse the returned values from vector quantiles query", nfe);
    }
    for (int i = 0; i < pArray.length; i++) {
      result[i] = mapValues.get(pArray[i]);
    }
    return result;
  }
}
