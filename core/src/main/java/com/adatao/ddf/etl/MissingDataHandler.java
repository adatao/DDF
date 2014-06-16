package com.adatao.ddf.etl;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.google.common.collect.Lists;

public class MissingDataHandler extends ADDFFunctionalGroupHandler implements IHandleMissingData {

  public MissingDataHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  // axis=0, how='any', thresh=None, subset=None, inplace=False
  //dropNA(0, "any", 0, null, false)
  @Override
  public DDF dropNA(int axis, String how, long thresh, List<String> subset, boolean inplace) throws DDFException {
    DDF newddf;

    int numcols = this.getDDF().getNumColumns();
    if (subset == null) {
      subset = this.getDDF().getColumnNames();
    }
    String sqlCmd = "";

    if (axis == 0) { // drop row with NA
      if (thresh > 0) {
        if (thresh > numcols) {
          throw new DDFException(
              "Required number of non-NA values per row must be less than or equal the number of columns.");
        } else {
          sqlCmd = dropNARowSQL(numcols - thresh + 1, subset);
        }
      } else if ("any".equalsIgnoreCase(how)) {
        sqlCmd = dropNARowSQL(1, subset);

      } else if ("all".equalsIgnoreCase(how)) {
        sqlCmd = dropNARowSQL(numcols, subset);
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
          cols = selectedColumns(numrows - thresh + 1, subset);
        }
      } else if ("any".equalsIgnoreCase(how)) {

        cols = selectedColumns(1, subset);
      } else if ("all".equalsIgnoreCase(how)) {
        cols = selectedColumns(1, subset);
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

  @Override
  public DDF fillNA() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF replaceNA() {
    // TODO Auto-generated method stub
    return null;
  }

  // value=None, method=None, axis=0, inplace=False, limit=None, downcast=None
  // to_replace=None, value=None, inplace=False, limit=None, regex=False, method='pad', axis=None



}
