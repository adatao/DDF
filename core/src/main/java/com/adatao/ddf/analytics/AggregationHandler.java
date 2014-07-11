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
import com.google.common.base.Joiner;

/**
 * 
 */
public class AggregationHandler extends ADDFFunctionalGroupHandler implements IHandleAggregation {

  private List<String> mGroupedColumns;


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
    mGroupedColumns = groupedColumns;
    return agg(aggregateFunctions);
  }

  @Override
  public DDF groupBy(List<String> groupedColumns) {
    mGroupedColumns = groupedColumns;
    return this.getDDF();
  }

  @Override
  public DDF agg(List<String> aggregateFunctions) throws DDFException {

    if (mGroupedColumns.size() > 0) {
      String tableName = this.getDDF().getTableName();

      String groupedColSql = Joiner.on(",").join(mGroupedColumns);

      String selectFuncSql = convertAggregateFunctionsToSql(aggregateFunctions.get(0));
      for (int i = 1; i < aggregateFunctions.size(); i++) {
        selectFuncSql += "," + convertAggregateFunctionsToSql(aggregateFunctions.get(i));
      }

      String sqlCmd = String.format("SELECT %s , %s FROM %s GROUP BY %s", selectFuncSql, groupedColSql, tableName,
          groupedColSql);
      mLog.info("SQL Command: " + sqlCmd);

      try {
        DDF resultDDF = this.getManager().sql2ddf(sqlCmd);
        this.getManager().addDDF(resultDDF);
        return resultDDF;

      } catch (Exception e) {
        e.printStackTrace();
        throw new DDFException("Unable to query from " + tableName, e);
      }
      
    } else {
      throw new DDFException("Need to set grouped columns before aggregation");
    }
  }
  
  private String convertAggregateFunctionsToSql(String s) {

    if(Strings.isNullOrEmpty(s)) return null;
    
    String[] splits = s.trim().split("=");
    if (splits.length == 2) {
      return AggregateField.fromFieldSpec(splits[1]).setName(splits[0]).toString();
    } else if (splits.length == 1) { // no name for aggregated value
      return AggregateField.fromFieldSpec(splits[0]).toString();
    }
    return s;
  }
}
