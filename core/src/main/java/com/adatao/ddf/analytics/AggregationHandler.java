package com.adatao.ddf.analytics;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.util.Utils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * 
 */
public class AggregationHandler extends ADDFFunctionalGroupHandler implements IHandleAggregation {

  public AggregationHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public double computeCorrelation(String columnA, String columnB) throws DDFException {
    if (!(this.getDDF().getColumn(columnA).isNumeric() || this.getDDF().getColumn(columnB).isNumeric())) throw new DDFException(
        "Only numeric fields are accepted!");
    String sqlCmd = String.format("SELECT CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF().getTableName());
    try {
      List<String> rs = this.getManager().sql2txt(sqlCmd);
      return Utils.roundUp(Double.parseDouble(rs.get(0)));
    } catch (Exception e) {
      throw new DDFException(String.format("Unable to get CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF()
          .getTableName()), e);
    }
  }

  /**
   * Performs the equivalent of a SQL aggregation statement like "SELECT year, month, AVG(depdelay), MIN(arrdelay) FROM
   * airline GROUP BY year, month"
   * 
   * @param fields
   *          {@link AggregateField}s representing a list of column specs, some of which may be aggregated, while other
   *          non-aggregated fields are the GROUP BY keys
   * @return
   * @throws DDFException
   */
  @Override
  public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {

    String tableName = this.getDDF().getTableName();

    String sqlCmd = AggregateField.toSql(fields, tableName);
    mLog.info("SQL Command: " + sqlCmd);
    int numUnaggregatedFields = 0;
    for (AggregateField field : fields) {
      if(!field.isAggregated()) numUnaggregatedFields ++;
    }
    try {
      List<String> result = this.getManager().sql2txt(sqlCmd);
      return AggregationResult.newInstance(result, numUnaggregatedFields);

    } catch (Exception e) {
      e.printStackTrace();
      throw new DDFException("Unable to query from " + tableName, e);
    }
  }


  // ////// Inner classes/enums ////////


  public static class AggregationResult extends HashMap<String, Double[]> {

    private static final long serialVersionUID = -7809562958792876728L;


    public static AggregationResult newInstance(List<String> sqlResult, int numUnaggregatedFields) {

      AggregationResult result = new AggregationResult();

      for (String res : sqlResult) {
        /*
        System.out.println("RESULT " + res);
        int pos = StringUtils.ordinalIndexOf(res, "\t", numFields - 1);
        String groupByColNames = res.substring(0, pos).replaceAll("\\t", ",");
        String[] stats = res.substring(pos + 1).split("\\t");
        */
        int pos = StringUtils.ordinalIndexOf(res, "\t", numUnaggregatedFields);
        String groupByColNames = res.substring(0, pos).replaceAll("\t", ",");
        String[] stats = res.substring(pos + 1).split("\t");

        Double[] statsDouble = new Double[stats.length];

        for (int i = 0; i < stats.length; i++) {
          if (!"null".equalsIgnoreCase(stats[i])) {
            statsDouble[i] = Double.NaN;

          } else {
            statsDouble[i] = Utils.roundUp(Double.parseDouble(stats[i]));
          }
        }

        result.put(groupByColNames, statsDouble);
      }

      return result;
    }
  }

  /**
   * Represents a field in the aggregation statement "SELECT a, b, SUM(c), MIN(c), MAX(d), COUNT(*) GROUP BY a, b"
   */
  public static class AggregateField {
    public String mColumn;
    public AggregateFunction mAggregateFunction;


    /**
     * An unaggregated column
     * 
     * @param column
     */
    public AggregateField(String column) {
      this((AggregateFunction) null, column);
    }

    /**
     * An aggregated column
     * 
     * @param column
     * @param aggregationFunction
     *          if null, then this is an unaggregated column
     */
    public AggregateField(String aggregateFunction, String column) {
      this(AggregateFunction.fromString(aggregateFunction), column);
    }

    public AggregateField(AggregateFunction aggregateFunction, String column) {
      mColumn = column;
      if (Strings.isNullOrEmpty(mColumn)) mColumn = "*";
      mAggregateFunction = aggregateFunction;
    }

    public boolean isAggregated() {
      return (mAggregateFunction != null);
    }

    public String getColumn() {
      return mColumn;
    }

    public AggregateFunction getAggregateFunction() {
      return mAggregateFunction;
    }

    @Override
    public String toString() {
      return this.isAggregated() ? String.format("%s(%s)", this.getAggregateFunction().toString(), this.getColumn())
          : this.getColumn();
    }

    /**
     * Helper method to convert an array of {@link AggregateField}s into a single SELECT statement like
     * "SELECT a, b, SUM(c), MIN(c), MAX(d), COUNT(*) GROUP BY a, b"
     * 
     * @param fields
     * @return
     * @throws DDFException
     */
    public static String toSql(List<AggregateField> fields, String tableName) throws DDFException {
      if (fields == null || fields.size() == 0) throw new DDFException("Field array cannot be null or empty");
      if (Strings.isNullOrEmpty(tableName)) throw new DDFException("Table name cannot be null or empty");

      return String.format("SELECT %s FROM %s GROUP BY %s", toSqlFieldSpecs(fields), tableName,
          toSqlGroupBySpecs(fields));
    }

    /**
     * Converts from a SQL String specs like "a, b, SUM(c), MIN(c)" into an array of SQL {@link AggregateField}s. This
     * is useful for constructing arguments to the {@link aggregate} function.
     * 
     * @param sqlFieldSpecs
     * @return null if sqlFieldSpecs is null or empty
     */
    public static List<AggregateField> fromSqlFieldSpecs(String sqlFieldSpecs) {
      if (Strings.isNullOrEmpty(sqlFieldSpecs)) return null;

      String[] specs = sqlFieldSpecs.split(",");
      List<AggregateField> fields = new ArrayList<AggregateField>();
      for (String spec : specs) {
        if (Strings.isNullOrEmpty(spec)) continue;

        spec = spec.trim();
        String[] parts = spec.split("\\(");
        if (parts.length == 1) {
          fields.add(new AggregateField(parts[0])); // just column name

        } else {
          fields.add(new AggregateField(parts[0], parts[1].replaceAll("\\)", ""))); // function(columnName)
        }
      }

      return fields;
    }

    private static String toSqlFieldSpecs(List<AggregateField> fields) {
      return toSqlSpecs(fields, true);
    }

    private static String toSqlGroupBySpecs(List<AggregateField> fields) {
      return toSqlSpecs(fields, false);
    }

    /**
     * 
     * @param fields
     * @param isFieldSpecs
     *          If true, include all fields. If false, include only unaggregated fields.
     * @return
     */
    private static String toSqlSpecs(List<AggregateField> fields, boolean isFieldSpecs) {
      List<String> specs = Lists.newArrayList();

      for (AggregateField field : fields) {
        if (isFieldSpecs || !field.isAggregated()) specs.add(field.toString());
      }

      return StringUtils.join(specs.toArray(), ',');
    }
  }


  public enum AggregateFunction {
    MEAN, COUNT, SUM, MIN, MAX, MEDIAN, VARIANCE, STDDEV;

    public static AggregateFunction fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (AggregateFunction t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }

    public String toString(String column) {
      switch (this) {
        case MEDIAN:
          return String.format("PERCENTILE_APPROX(%s, 0.5)", column);

        case MEAN:
          return String.format("AVG(%s)", column);

        default:
          return String.format("%s(%s)", this.toString(), column);
      }

    }
  }
}
