package com.adatao.ddf.content;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;

/**
 * Table schema of a DDF including table name and column metadata
 * 
 * @author bhan
 * 
 */
@SuppressWarnings("serial")
public class Schema implements Serializable {
  private String mTableName;
  private List<Column> mColumns = Lists.newArrayList();

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

  private void initialize(String tableName, List<Column> columns) {
    if (tableName == null) tableName = UUID.randomUUID().toString();
    this.mTableName = tableName;
    this.mColumns = columns;

  }

  private List<Column> parseColumnList(String columnList) {
    /* TODO */
    return null;
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
    Integer i = getColumnIndex(name);
    if (i == null) {
      return null;
    }

    return getColumn(i);
  }

  public Integer getColumnIndex(String name) {
    if (mColumns.isEmpty()) {
      return null;
    }
    for (int i = 0; i < mColumns.size(); i++) {
      if (mColumns.get(i).getName().equals(name)) {
        return i;
      }
    }
    return null;
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
    if (getColumnIndex(name) != null) {
      this.mColumns.remove(getColumnIndex(name));
      return true;
    } else {
      return false;
    }

  }

  /**
   * Remove a column by its index
   * 
   * @param i
   *          Column index
   * @return true if succeed
   */
  public boolean removeColumn(int i) {
    if (getColumn(i) != null) {
      this.mColumns.remove(i);
      return true;
    } else {
      return false;
    }
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

  }

  public enum ColumnType {

    STRING, INTEGER, DOUBLE, FLOAT, TIMESTAMP, BLOB;

    public static ColumnType fromString(String s) {
      if (s == null || s.length() == 0) return null;

      s = s.toUpperCase().trim();

      for (ColumnType t : values()) {
        if (s.equals(t.name())) return t;
      }

      return null;
    }
  }

  public enum DataFormat {
    SQL, CSV, TSV
  }
}
