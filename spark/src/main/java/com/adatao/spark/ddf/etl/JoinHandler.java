package com.adatao.spark.ddf.etl;


import java.util.HashSet;
import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDF.JoinType;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.etl.IHandleJoins;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.google.gson.annotations.SerializedName;

public class JoinHandler extends ADDFFunctionalGroupHandler implements IHandleJoins {


  public JoinHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
      List<String> byRightColumns) {

    String leftTableName = getDDF().getTableName();
    String rightTableName = anotherDDF.getTableName();
    List<Column> rightColumns = anotherDDF.getSchema().getColumns();
    HashSet<String> rightColumNameSet = new HashSet<String>();
    for (Column m : rightColumns) {
      rightColumNameSet.add(m.getName());
    }

    String joinSqlCommand = "SELECT lt.*,%s FROM %s lt %s JOIN %s rt ON (%s)";
    String joinLeftSemiCommand = "SELECT lt.* FROM %s lt %s JOIN %s rt ON (%s)";
    String columnString = "";

    if (byColumns != null && byColumns.size() > 0) {
      for (int i = 0; i < byColumns.size(); i++) {
        columnString += String.format("lt.%s = rt.%s AND ", byColumns.get(i), byColumns.get(i));
        rightColumNameSet.remove(byColumns.get(i));
      }
    } else {
      if (byLeftColumns != null && byRightColumns != null && byLeftColumns.size() == byRightColumns.size()
          && byLeftColumns.size() > 0) {
        for (int i = 0; i < byLeftColumns.size(); i++) {
          columnString += String.format("lt.%s = rt.%s AND ", byLeftColumns.get(i), byRightColumns.get(i));
          rightColumNameSet.remove(byRightColumns.get(i));
        }
      } else {
        // TODO throw exceptions
      }
    }
    columnString = columnString.substring(0, columnString.length() - 5);

    // we will not select column that is already in left table
    String rightSelectColumns = "";
    for (String name : rightColumNameSet) {
      rightSelectColumns += String.format("rt.%s AS r_%s,", name, name);
    }
    rightSelectColumns = rightSelectColumns.substring(0, rightSelectColumns.length() - 1);

    try {
      if (joinType == JoinType.LEFTSEMI) {
        joinSqlCommand = String.format(joinLeftSemiCommand, leftTableName, joinType.getStringRepr(), rightTableName,
            columnString);
      } else {
        joinSqlCommand = String.format(joinSqlCommand, rightSelectColumns, leftTableName, joinType.getStringRepr(),
            rightTableName, columnString);
      }

    } catch (Exception ex) {
      System.err.println("Error while building join sql");
    }
    
    System.out.println(">>> joinSqlCommand = " + joinSqlCommand);
    
    
    try {
      DDF resultDDF = this.getManager().sql2ddf(joinSqlCommand);
      this.getDDF().getManager().addDDF(resultDDF);
      return resultDDF;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

  }
}
