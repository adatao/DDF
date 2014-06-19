package com.adatao.ddf.etl;


import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.content.Schema.ColumnType;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.types.AggregateTypes.AggregateFunction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Handle missing data, based on NA handling methods in Pandas DataFrame
 * 
 * @author bhan
 * 
 */
public class MissingDataHandler extends ADDFFunctionalGroupHandler implements IHandleMissingData {

  public MissingDataHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  /**
   * This function drop rows or columns that contain NA values, Default: axis=0, how='any', thresh=0, columns=null,
   * inplace=false
   * 
   * @param axis
   *          = 0: drop by row, 1: drop by column, default 0
   * @param how
   *          = 'any' or 'all', default 'any'
   * @param thresh
   *          = required number of non-NA values to skip, default 0
   * @param columns
   *          = only consider NA dropping on the given columns, set to null for all columns of the DDF, default null
   * @param inplace
   *          = false: result in new DDF, true: update on the same DDF, default false
   * @return a DDF with dropNA handled
   */
  @Override
  public DDF dropNA(int axis, String how, long thresh, List<String> columns, boolean inplace) throws DDFException {
    DDF newddf = null;

    int numcols = this.getDDF().getNumColumns();
    if (columns == null) {
      columns = this.getDDF().getColumnNames();
    }
    String sqlCmd = "";

    if (axis == 0) { // drop row with NA
      if (thresh > 0) {
        if (thresh > numcols) {
          throw new DDFException(
              "Required number of non-NA values per row must be less than or equal the number of columns.");
        } else {
          sqlCmd = dropNARowSQL(numcols - thresh + 1, columns);
        }
      } else if ("any".equalsIgnoreCase(how)) {
        sqlCmd = dropNARowSQL(1, columns);

      } else if ("all".equalsIgnoreCase(how)) {
        sqlCmd = dropNARowSQL(numcols, columns);
      }

      newddf = this.getManager().sql2ddf(String.format(sqlCmd, this.getDDF().getTableName()));

    } else if (axis == 1) { // drop column with NA
      List<String> cols = Lists.newArrayList();
      long numrows = this.getDDF().getNumRows();
      if (thresh > 0) {
        if (thresh > numrows) {
          throw new DDFException(
              "Required number of non-NA values per column must be less than or equal the number of rows.");
        } else {
          cols = selectedColumns(numrows - thresh + 1, columns);
        }
      } else if ("any".equalsIgnoreCase(how)) {

        cols = selectedColumns(1, columns);
      } else if ("all".equalsIgnoreCase(how)) {
        cols = selectedColumns(1, columns);
      }

      newddf = this.getDDF().Views.project(cols);

    } else {
      throw new DDFException(
          "Either choose axis = 0 for row-based NA filtering or axis = 1 for column-based NA filtering");
    }


    if (inplace) {
      return this.getDDF().updateInplace(newddf);

    } else {
      this.getManager().addDDF(newddf);
      return newddf;
    }

  }

  private String dropNARowSQL(long thresh, List<String> columns) {
    StringBuffer caseCmd = new StringBuffer("(");


    for (String col : columns) {

      caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN 1 ELSE 0 END) +", col));
    }

    caseCmd.setLength(caseCmd.length() - 1); // remove the last "+"
    caseCmd.append(")");

    caseCmd.append(String.format("< %s", thresh));


    String sqlCmd = String.format("SELECT * FROM %%s WHERE %s", caseCmd.toString());
    return sqlCmd;
  }

  private List<String> selectedColumns(long thresh, List<String> columns) throws DDFException {
    List<String> cols = Lists.newArrayList();
    for (String column : columns) {
      if (numberOfNAPerColumn(column) < thresh) {
        cols.add(column);
      }
    }
    return cols;
  }

  private long numberOfNAPerColumn(String column) throws DDFException {
    return Long
        .parseLong(this.getDDF()
            .sql2txt(String.format("SELECT COUNT(*) FROM %%s WHERE %s IS NULL", column), "Unable to run the query.")
            .get(0));
  }


  /**
   * This function fills NA with given values. Default using a scalar value fillNA(value,null, 0, null, null, null,
   * false)
   * 
   * @param value
   *          a scalar value to fill all NAs
   * @param method
   *          = 'ffill' for forward fill or 'bfill' for backward fill
   * @param limit
   *          = maximum size gap for forward or backward fill
   * @param function
   *          aggregate function to generate the filled value for a column
   * @param columnsToValues
   *          = a map to provide different values to fill for different columns
   * @param columns
   *          = only consider NA filling on the given columns, set to null for all columns of the DDF
   * @param inplace
   *          = false: result in new DDF, true: update on the same DDF
   * @return a DDF with fillNA handled
   */
  @Override
  public DDF fillNA(String value, String method, long limit, String function, Map<String, String> columnsToValues,
      List<String> columns, boolean inplace) throws DDFException {

    DDF newddf = null;
    if (columns == null) {
      columns = this.getDDF().getColumnNames();
    }

    if (Strings.isNullOrEmpty(method)) {
      String sqlCmd = fillNAWithValueSQL(value, function, columnsToValues, columns);
      mLog.info("FillNA sql command: " + sqlCmd);
      newddf = this.getManager().sql2ddf(String.format(sqlCmd, this.getDDF().getTableName()));

    } else { // interpolation methods 'ffill' or 'bfill'
      // TODO:
    }


    if (inplace) {
      return this.getDDF().updateInplace(newddf);

    } else {
      this.getManager().addDDF(newddf);
      return newddf;
    }

  }

  private String fillNAWithValueSQL(String value, String function, Map<String, String> columnsToValues,
      List<String> columns) throws DDFException {

    StringBuffer caseCmd = new StringBuffer("");
    for (String col : columns) {
      if (!Strings.isNullOrEmpty(value)) { // fill by value

        if (this.getDDF().getColumn(col).isNumeric()) {
          caseCmd.append(fillNACaseSql(col, value));
        } else {
          caseCmd.append(fillNACaseSql(col, String.format("'%s'", value)));
        }

      } else if (MapUtils.isNotEmpty(columnsToValues)) { // fill different values for different columns
        Set<String> keys = columnsToValues.keySet();

        if (keys.contains(col)) {
          String filledValue = columnsToValues.get(col);
          if (this.getDDF().getColumn(col).isNumeric()) {
            caseCmd.append(fillNACaseSql(col, filledValue));
          } else {
            caseCmd.append(fillNACaseSql(col, String.format("'%s'", filledValue)));
          }
        } else {
          caseCmd.append(String.format("%s,", col));
        }
      } else {// fill by function
        if (AggregateFunction.fromString(function) != null) {// fill by function
          Column curColumn = this.getDDF().getColumn(col);
          if (this.getDDF().getColumn(col).isNumeric()) {
            double filledValue = this.getDDF().getAggregationHandler().aggregateOnColumn(function, col);
            if (curColumn.getType() == ColumnType.DOUBLE) {
              caseCmd.append(fillNACaseSql(col, filledValue));
            } else {
              caseCmd.append(fillNACaseSql(col, Math.round(filledValue)));
            }
            
          } else {
            caseCmd.append(String.format("%s,", col));
          }
        } else {
          throw new DDFException("Unsupported or incorrect aggregate function.");
        }
      }
    }
    caseCmd.setLength(caseCmd.length() - 1); // remove the last "+"
    String sqlCmd = String.format("SELECT %s FROM %%s", caseCmd.toString());
    return sqlCmd;
  }

  private String fillNACaseSql(String column, String filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }

  private String fillNACaseSql(String column, long filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }
  
  private String fillNACaseSql(String column, double filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }

  @Override
  public DDF replaceNA() {
    // TODO Auto-generated method stub
    return null;
  }

}
