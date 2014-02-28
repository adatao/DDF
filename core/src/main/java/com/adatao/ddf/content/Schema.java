package com.adatao.ddf.content;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import scala.actors.threadpool.Arrays;

import com.google.common.base.Strings;

/**
 * Table schema of a DDF including table name and column metadata
 */
@SuppressWarnings("serial")
public class Schema implements Serializable {
  private String mTableName;
  private List<Column> mColumns = Collections.synchronizedList(new ArrayList<Column>());

  /**
   * Constructor that can take a list of columns in the following format:
   * "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float". This string will
   * be parsed into a {@link List} of {@link Column}s.
   * 
   * Since the table name is not specified, it is initially set to a random UUID.
   * 
   * @param columns
   */
  public Schema(String columns) {
    this.initialize(null, this.parseColumnList(columns));
  }

  /**
   * Constructor that can take a list of columns in the following format:
   * "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float".
   * 
   * @param tableName
   * @param columns
   */
  public Schema(String tableName, String columns) {
    this.initialize(tableName, this.parseColumnList(columns));
  }

  public Schema(List<Column> columns) {
    this.initialize(null, columns);
  }

  public Schema(String tableName, List<Column> columns) {
    this.initialize(tableName, columns);
  }

  @SuppressWarnings("unchecked")
  public Schema(String tableName, Column[] columns) {
    this.initialize(tableName, Arrays.asList(columns));
  }

  private void initialize(String tableName, List<Column> columns) {
    if (Strings.isNullOrEmpty(tableName)) tableName = UUID.randomUUID().toString();
    this.mTableName = tableName;
    this.mColumns = columns;

  }

  //"<name> <type>, <name> <type>"
  private List<Column> parseColumnList(String columnList) {
    
    if (Strings.isNullOrEmpty(columnList)) return null;
    String[] segments = columnList.split(" *, *");

    mColumns.clear();
    for (String segment : segments) {
      if (Strings.isNullOrEmpty(segment)) continue;

      String[] parts = segment.split("  *");
      if (Strings.isNullOrEmpty(parts[0]) || Strings.isNullOrEmpty(parts[1])) continue;

      mColumns.add(new Column(parts[0], parts[1]));
    }

    return mColumns;
  }

  public String getTableName() {
    return mTableName;
  }

  public void setTableName(String mTableName) {
    this.mTableName = mTableName;
  }

  public List<Column> getColumns() {
    return mColumns;
  }

  public void setColumns(List<Column> Columns) {
    this.mColumns = Columns;
  }

  public void setColumnNames(List<String> names) {
    int length = names.size() < mColumns.size() ? names.size() : mColumns.size();
    for (int i = 0; i < length; i++) {
      mColumns.get(i).setName(names.get(i));
    }
  }

  public Column getColumn(int i) {
    if (mColumns.isEmpty()) {
      return null;
    }
    if (i < 0 || i >= mColumns.size()) {
      return null;
    }

    return mColumns.get(i);
  }

  public Column getColumn(String name) {
    int i = getColumnIndex(name);
    if (i == -1) {
      return null;
    }
    return getColumn(i);
  }

  public int getColumnIndex(String name) {

    if (mColumns.isEmpty() || Strings.isNullOrEmpty(name)) return -1;

    for (int i = 0; i < mColumns.size(); i++) {
      if (name.equalsIgnoreCase(mColumns.get(i).getName())) return i;
    }

    return -1;
  }

  /**
   * 
   * @return number of columns
   */
  public int getNumColumns() {
    return this.mColumns.size();
  }

  public void addColumn(Column col) {
    this.mColumns.add(col);
  }

  /**
   * Remove a column by its name
   * 
   * @param name
   *          Column name
   * @return true if succeed
   */
  public boolean removeColumn(String name) {

    if (getColumnIndex(name) < 0) return false;
    this.mColumns.remove(getColumnIndex(name));
    return true;

  }

  /**
   * Remove a column by its index
   * 
   * @param i
   *          Column index
   * @return true if succeed
   */
  public boolean removeColumn(int i) {
    if (getColumn(i) == null) return false;
    this.mColumns.remove(i);
    return true;
  }

  /**
   * This class represents the metadata of a column
   * 
   * 
   */
  public static class Column {
    private String mName;
    private ColumnType mType;

    public Column(String name, ColumnType type) {
      this.mName = name;
      this.mType = type;
    }

    public Column(String name, String type) {
      this(name, ColumnType.fromString(type));
    }

    public String getName() {
      return mName;
    }

    public Column setName(String name) {
      this.mName = name;
      return this;
    }

    public ColumnType getType() {
      return mType;
    }

    public Column setType(ColumnType type) {
      this.mType = type;
      return this;
    }
    
    public boolean isNumeric() {
      return ColumnType.isNumeric(mType);
    }

  }

  public static class ColumnWithData extends Column {
    private Object[] mData;

    public ColumnWithData(String name, Object[] data) {
      super(name, ColumnType.fromArray(data));
    }

    /**
     * @return the data
     */
    public Object[] getData() {
      return mData;
    }

    /**
     * @param data
     *          the data to set
     */
    public void setData(Object[] data) {
      this.mData = data;
    }
  }

  public enum ColumnType {

    STRING, INT, LONG, FLOAT, DOUBLE, TIMESTAMP, BLOB;

    public static ColumnType fromString(String s) {
      if (s == null || s.length() == 0) return null;
      s = s.toUpperCase().trim();
      for (ColumnType t : values()) {
        if (s.equals(t.name())) return t;
      }
      return null;
    }

    public static ColumnType fromObject(Object obj) {
      if (obj == null) return null;
      Class<?> objClass = obj.getClass();
      if (String.class.isAssignableFrom(objClass)) return STRING;
      if (Integer.class.isAssignableFrom(objClass)) return DOUBLE;
      if (Long.class.isAssignableFrom(objClass)) return LONG;
      if (Float.class.isAssignableFrom(objClass)) return FLOAT;
      if (Date.class.isAssignableFrom(objClass)) return TIMESTAMP;
      if (java.sql.Date.class.isAssignableFrom(objClass)) return TIMESTAMP;
      if (Timestamp.class.isAssignableFrom(objClass)) return TIMESTAMP;
      else return BLOB;
    }

    public static ColumnType fromArray(Object[] elements) {
      return (elements == null ? null : fromObject(elements[0]));
    }
    
    public static boolean isNumeric(ColumnType colType) {
      switch (colType) {
        case INT: return true;
        case DOUBLE: return true;
        case FLOAT: return true;
        case LONG: return true;  
        default: return false;
      } 
    }
  }

  public enum DataFormat {
    SQL, CSV, TSV
  }
}
