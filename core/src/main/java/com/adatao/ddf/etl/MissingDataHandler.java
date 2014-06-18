package com.adatao.ddf.etl;


import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import com.adatao.ddf.DDF;
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

  // axis=0, how='any', thresh=None, subset=None, inplace=False
  // dropNA(0, "any", 0, null, false)
  /**
   * how: 'any' or 'all'
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

  // value=None, method=None, inplace=False, limit=None, downcast=None
  // fill column-by-column
  @Override
  public DDF fillNA(String value, String method, long limit, String function, Map<String, String> columnsToValues,
      List<String> columns, boolean inplace) throws DDFException {

    DDF newddf = null;
    if (columns == null) {
      columns = this.getDDF().getColumnNames();
    }

    if (Strings.isNullOrEmpty(method)) {
      String sqlCmd = fillNAWithValueSQL(value, function, columnsToValues, columns);
      newddf = this.getManager().sql2ddf(String.format(sqlCmd, this.getDDF().getTableName()));

    } else { // interpolation methods 'ffill' or 'bfill'
      // TODO:
    }

    // if (axis == 0) { // fill column-by-column
    //
    // } else if (axis == 1) { // fill row-by-row
    //
    // } else {
    // throw new DDFException(
    // "Either choose axis = 0 for row-based NA filtering or axis = 1 for column-based NA filtering");
    // }
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
          caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END),", col, value, col));
        } else {
          caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END),", col,
              String.format("'%s'", value), col));
        }

      } else if (MapUtils.isNotEmpty(columnsToValues)) { // fill different values for different columns
        Set<String> keys = columnsToValues.keySet();

        if (keys.contains(col)) {
          String filledValue = columnsToValues.get(col);
          if (this.getDDF().getColumn(col).isNumeric()) {
            caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END),", col, filledValue, col));
          } else {
            caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END),", col,
                String.format("'%s'", filledValue), col));
          }
        } else {
          caseCmd.append(String.format("%s,", col));
        }
      } else {// fill by function
        if (AggregateFunction.fromString(function) != null) {// fill by function
          if (this.getDDF().getColumn(col).isNumeric()) {
            double filledValue = this.getDDF().getAggregationHandler().aggregateOnColumn(function, col);
            caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END),", col, filledValue, col));
          } else {
            caseCmd.append(String.format("%s,", col));
          }
        } else {
          throw new DDFException("Unsupported or incorrect aggregate function.");
        }
      }
    }
    caseCmd.setLength(caseCmd.length() - 1); // remove the last "+"
    String sqlCmd = String.format("SELECT * FROM %%s WHERE %s", caseCmd.toString());
    return sqlCmd;
  }

  @Override
  public DDF replaceNA() {
    // TODO Auto-generated method stub
    return null;
  }


  // to_replace=None, value=None, inplace=False, limit=None, regex=False, method='pad', axis=None



}
