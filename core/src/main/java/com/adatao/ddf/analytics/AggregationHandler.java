package com.adatao.ddf.analytics;


import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.util.Utils;
import com.adatao.ddf.types.AggregateTypes.*;
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
    if (!(this.getDDF().getColumn(columnA).isNumeric() || this.getDDF().getColumn(columnB).isNumeric())) {
      throw new DDFException("Only numeric fields are accepted!");
    }

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
      if (!field.isAggregated()) numUnaggregatedFields++;
    }

    try {
      List<String> result = this.getManager().sql2txt(sqlCmd);
      return AggregationResult.newInstance(result, numUnaggregatedFields);

    } catch (Exception e) {
      e.printStackTrace();
      throw new DDFException("Unable to query from " + tableName, e);
    }
  }

  @Override
  public AggregationResult xtabs(List<AggregateField> fields) throws DDFException {
    return this.aggregate(fields);
  }


  @Override
  public double aggregateOnColumn(AggregateFunction function, String column) throws DDFException {
    return Double.parseDouble(this.getManager()
        .sql2txt(String.format("SELECT %s from %s", function.toString(column), this.getDDF().getTableName())).get(0));
  }

  @Override
  public DDF groupBy(List<String> groupedColumns, List<String> aggregateFunctions) throws DDFException {

    String tableName = this.getDDF().getTableName();

    String groupedColSql = groupedColumns.get(0);
    for (int i = 1; i < groupedColumns.size(); i++) {
      groupedColSql += "," + groupedColumns.get(i);
    }

    String selectFuncSql = convertFunc2Sql(aggregateFunctions.get(0));
    for (int i = 1; i < aggregateFunctions.size(); i++) {
      selectFuncSql += "," + convertFunc2Sql(aggregateFunctions.get(i));
    }

//    if(aggregateFunctions.size() > 0 && groupedColumns.size() > 0) groupedColSql = "," + groupedColSql;
    
    String sqlCmd = String.format("SELECT %s , %s FROM %s GROUP BY %s", selectFuncSql, groupedColSql, tableName, groupedColSql);
    mLog.info("SQL Command: " + sqlCmd);

    try {
      DDF resultDDF = this.getManager().sql2ddf(sqlCmd);
      this.getManager().addDDF(resultDDF);
      return resultDDF;

    } catch (Exception e) {
      e.printStackTrace();
      throw new DDFException("Unable to query from " + tableName, e);
    }
  }

  private String convertFunc2Sql(String s) {
    if (s == null) return s;

    String[] splits = s.trim().split("=");
    if (splits != null && splits.length == 2) {
      return splits[1] + " AS " + splits[0];
    }
    return s;
  }


}
