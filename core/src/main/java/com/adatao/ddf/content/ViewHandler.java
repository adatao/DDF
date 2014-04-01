/**
 * 
 */
package com.adatao.ddf.content;


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.google.common.base.Joiner;

/**
 * 
 */
public class ViewHandler extends ADDFFunctionalGroupHandler implements IHandleViews {

  public ViewHandler(DDF theDDF) {
    super(theDDF);
  }

  // @SuppressWarnings("unchecked")
  // @Override
  // public <T> Iterator<T> getRowIterator(Class<T> dataType) {
  // if (dataType == null) dataType = (Class<T>) this.getDDF().getRepresentationHandler().getDefaultDataType();
  //
  // Object repr = this.getDDF().getRepresentationHandler().get(dataType);
  // return (repr instanceof Iterable<?>) ? ((Iterable<T>) repr).iterator() : null;
  // }

  @Override
  public List<Object[]> getRandomSample(int numSamples, boolean withReplacement, int seed) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
    // TODO Auto-generated method stub
    return null;
  }


  // public static class ElementIterator<C> implements Iterator<C> {
  //
  // private Iterator<?> mRowIterator;
  // private int mColumnIndex;
  //
  //
  // public ElementIterator(Iterator<?> rowIterator, int columnIndex) {
  // mRowIterator = rowIterator;
  // mColumnIndex = columnIndex;
  // }
  //
  // @Override
  // public boolean hasNext() {
  // return mRowIterator.hasNext();
  // }
  //
  // @SuppressWarnings("unchecked")
  // @Override
  // public C next() {
  // Object row = mRowIterator.next();
  // if (row == null) return null;
  //
  // if (row.getClass().isArray()) {
  // C[] x = (C[]) row;
  // return x[mColumnIndex];
  //
  // } else if (List.class.isAssignableFrom(row.getClass())) {
  // return ((List<C>) row).get(mColumnIndex);
  //
  // } else {
  // return null;
  // }
  // }
  //
  // @Override
  // public void remove() {
  // // Not supported
  // }
  // }


  // /**
  // * The base implementation supports the case where the dataType is an Array or List
  // */
  // @SuppressWarnings("unchecked")
  // @Override
  // public <C> Iterator<C> getElementIterator(Class<?> dataType, Class<C> columnType, int columnIndex) {
  // if (dataType == null) {
  // Object data = this.getDDF().getRepresentationHandler().getDefault();
  // if (data == null) return null;
  // dataType = data.getClass();
  // }
  //
  // if (columnType == null) columnType = (Class<C>) this.getDDF().getRepresentationHandler().getDefaultColumnType();
  //
  // if (Iterable.class.isAssignableFrom(dataType) || dataType.isArray()) {
  // Iterator<?> rowIterator = this.getRowIterator(dataType);
  // return new ElementIterator<C>(rowIterator, columnIndex);
  //
  // } else {
  // return null;
  // }
  // }
  //
  // @Override
  // public <C> Iterator<C> getElementIterator(Class<?> dataType, Class<C> columnType, String columnName) {
  // return this.getElementIterator(dataType, columnType, this.getDDF().getColumnIndex(columnName));
  // }
  //
  // @Override
  // public Iterator<?> getRowIterator() {
  // return this.getRowIterator(null);
  // }
  //
  // @Override
  // public Iterator<?> getElementIterator(int columnIndex) {
  // return this.getElementIterator(null, null, columnIndex);
  // }
  //
  // @Override
  // public Iterator<?> getElementIterator(String columnName) {
  // return this.getElementIterator(null, null, this.getDDF().getColumnIndex(columnName));
  // }

  @Override
  public List<String> firstNRows(int numRows) throws DDFException {
    return this.getDDF().sql2txt(String.format("SELECT * FROM %%s LIMIT %d", numRows),
        String.format("Unable to fetch %d rows from table %%s", numRows));
  }

  @Override
  public DDF project(String... columnNames) throws DDFException {
    if (columnNames == null || columnNames.length == 0) throw new DDFException("columnNames must be specified");

    String selectedColumns = Joiner.on(",").join(columnNames);
    return sql2ddf(String.format("SELECT %s FROM %%s", selectedColumns),
        String.format("Unable to project columns %s from table %%s", selectedColumns));
  }


  // ///// Execute SQL command on the DDF ///////

  private DDF sql2ddf(String sqlCommand, String errorMessage) throws DDFException {
    try {
      return this.getManager().sql2ddf(String.format(sqlCommand, this.getDDF().getTableName()));

    } catch (Exception e) {
      throw new DDFException(String.format(errorMessage, this.getDDF().getTableName()), e);
    }
  }

  @Override
  public DDF subset(List<ColumnExpression> columnExpr, Expression filter) throws DDFException {
    updateVectorName(filter, this.getDDF());
    mLog.info("Updated filter: " + filter);

    String[] colNames = new String[columnExpr.size()];
    for (int i = 0; i < columnExpr.size(); i++) {
      updateVectorName(columnExpr.get(i), this.getDDF());
      colNames[i] = columnExpr.get(i).getName();
    }
    mLog.info("Updated columns: " + Arrays.toString(columnExpr.toArray()));

    String sqlCmd = String.format("SELECT %s FROM %s", Joiner.on(", ").join(colNames), this.getDDF().getTableName());
    if (filter != null) {
      sqlCmd = String.format("%s WHERE %s", sqlCmd, filter.toSql());
    }
    mLog.info("sql = {}", sqlCmd);

    DDF subset = this.getManager().sql2ddf(sqlCmd);

    return subset;

  }


  /**
   * Base class for any Expression node in the AST, could be either an Operator or a Value
   * 
   */
  static public class Expression implements Serializable {
    String type;


    public String getType() {
      return type;
    }

    public String toSql() {
      return null;
    }

    public void setType(String aType) {
      type = aType;
    }
  }

  public enum OperationName {
    lt, le, eq, ge, gt, ne, and, or, neg, isnull, isnotnull
  }

  /**
   * Base class for unary operations and binary operations
   * 
   */
  static public class Operator extends Expression {
    OperationName name;
    Expression[] operands;


    public OperationName getName() {
      return name;
    }


    public Expression[] getOperands() {
      return operands;
    }

    @Override
    public String toString() {
      return "Operator [name=" + name + ", operands=" + Arrays.toString(operands) + "]";
    }

    @Override
    public String toSql() {
      if (name == null) {
        throw new IllegalArgumentException("Missing Operator name from Adatao client for operands[] "
            + Arrays.toString(operands));
      }
      switch (name) {
        case gt:
          return String.format("(%s > %s)", operands[0].toSql(), operands[1].toSql());
        case lt:
          return String.format("(%s < %s)", operands[0].toSql(), operands[1].toSql());
        case ge:
          return String.format("(%s >= %s)", operands[0].toSql(), operands[1].toSql());
        case le:
          return String.format("(%s <= %s)", operands[0].toSql(), operands[1].toSql());
        case eq:
          return String.format("(%s == %s)", operands[0].toSql(), operands[1].toSql());
        case ne:
          return String.format("(%s != %s)", operands[0].toSql(), operands[1].toSql());
        case and:
          return String.format("(%s AND %s)", operands[0].toSql(), operands[1].toSql());
        case or:
          return String.format("(%s OR %s)", operands[0].toSql(), operands[1].toSql());
        case neg:
          return String.format("(NOT %s)", operands[0].toSql());
        case isnull:
          return String.format("(%s IS NULL)", operands[0].toSql());
        case isnotnull:
          return String.format("(%s IS NOT NULL)", operands[0].toSql());
        default:
          throw new IllegalArgumentException("Unsupported Operator: " + name);
      }
    }
  }

  public abstract static class Value extends Expression {
    public abstract Object getValue();
  }

  static public class IntVal extends Value {
    int value;


    @Override
    public String toString() {
      return "IntVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Integer.toString(value);
    }
  }

  static public class DoubleVal extends Value {
    double value;


    @Override
    public String toString() {
      return "DoubleVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Double.toString(value);
    }
  }

  static public class StringVal extends Value {
    String value;


    @Override
    public String toString() {
      return "StringVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return String.format("'%s'", value);
    }
  }

  static public class BooleanVal extends Value {
    Boolean value;


    @Override
    public String toString() {
      return "BooleanVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Boolean.toString(value);
    }
  }

  static public class ColumnExpression extends Expression {
    String id;
    String name;
    Integer index = null;


    public String getID() {
      return id;
    }

    public Integer getIndex() {
      return index;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setIndex(Integer index) {
      this.index = index;
    }

    public void setID(String id) {
      this.id = id;
    }

    public Object getValue(Object[] xs) {
      return xs[index];
    }

    @Override
    public String toSql() {
      assert this.name != null;
      return this.name;
    }

    @Override
    public String toString() {
      return "Column [id=" + id + ", name=" + name + ", index=" + index + "]";
    }

    public String getName() {
      return name;
    }
  }


  private com.adatao.ddf.content.Schema.Column[] selectColumnMetaInfo(List<ColumnExpression> columns, DDF ddf) {
    int length = columns.size();
    com.adatao.ddf.content.Schema.Column[] retObj = new com.adatao.ddf.content.Schema.Column[length];
    for (int i = 0; i < length; i++) {
      retObj[i] = ddf.getSchema().getColumn(columns.get(i).getIndex());
    }
    return retObj;
  }

  private void updateVectorIndex(Expression Expression, DDF ddf) {
    if (Expression == null) {
      return;
    }
    if (Expression.getType().equals("Column")) {
      ColumnExpression vec = (ColumnExpression) Expression;
      if (vec.getIndex() == null) {
        String name = vec.getName();
        if (name != null) {
          vec.setIndex(ddf.getColumnIndex(name));
        }
      }
      return;
    }
    if (Expression instanceof Operator) {
      Expression[] newOps = ((Operator) Expression).getOperands();
      for (Expression newOp : newOps) {
        updateVectorIndex(newOp, ddf);
      }
    }
  }

  private void updateVectorName(Expression Expression, DDF ddf) {
    if (Expression == null) {
      return;
    }
    if (Expression.getType().equals("ColumnExpression")) {
      ColumnExpression vec = (ColumnExpression) Expression;
      if (vec.getName() == null) {
        Integer i = vec.getIndex();
        if (i != null) {
          vec.setName(ddf.getSchema().getColumnName(i));
        }
      }
      return;
    }
    if (Expression instanceof Operator) {
      Expression[] newOps = ((Operator) Expression).getOperands();
      for (Expression newOp : newOps) {
        updateVectorName(newOp, ddf);
      }
    }
  }

}
