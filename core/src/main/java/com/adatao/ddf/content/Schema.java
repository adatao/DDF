package com.adatao.ddf.content;


import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import scala.actors.threadpool.Arrays;
import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.common.collect.Lists;

/**
 * Table schema of a DDF including table name and column metadata
 */
@SuppressWarnings("serial")
public class Schema implements Serializable {
  @Expose private String mTableName;
  @Expose private List<Column> mColumns = Collections.synchronizedList(new ArrayList<Column>());



  /**
   * Constructor that can take a list of columns in the following format: "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float". This string will be parsed into a
   * {@link List} of {@link Column}s.
   * 
   * @param columns
   */
  @Deprecated
  // Require tableName at all times, even null
  public Schema(String columns) {
    this.initialize(null, this.parseColumnList(columns));
  }

  /**
   * Constructor that can take a list of columns in the following format: "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float".
   * 
   * @param tableName
   * @param columns
   */
  public Schema(String tableName, String columns) {
    this.initialize(tableName, this.parseColumnList(columns));
  }

  @Deprecated
  // Require tableName at all times, even null
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
    this.mTableName = tableName;
    this.mColumns = columns;
  }

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

  public List<String> getColumnNames() {
    List<String> columnNames = Lists.newArrayList();
    for (Column col : mColumns) {
      columnNames.add(col.getName());
    }
    return columnNames;
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
    @Expose private String mName;
    @Expose private ColumnType mType;


    public Column(String name, ColumnType type) {
      this.mName = name;
      this.mType = type;
    }

    public Column(String name, String type) {
      this(name, ColumnType.get(type));
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
      super(name, ColumnType.get(data));
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

    STRING(String.class), INT(Integer.class), LONG(Long.class), FLOAT(Float.class), DOUBLE(Double.class), //
    TIMESTAMP(Date.class, java.sql.Date.class, Time.class, Timestamp.class), BLOB(Object.class);

    private List<Class<?>> mClasses = Lists.newArrayList();


    private ColumnType(Class<?>... classes) {
      if (classes != null && classes.length > 0) {
        for (Class<?> cls : classes) {
          mClasses.add(cls);
        }
      }
    }

    public List<Class<?>> getClasses() {
      return mClasses;
    }

    public static ColumnType get(String s) {
      if (s == null || s.length() == 0) return null;

      for (ColumnType type : values()) {
        if (type.name().equalsIgnoreCase(s)) return type;

        for (Class<?> cls : type.getClasses()) {
          if (cls.getSimpleName().equalsIgnoreCase(s)) return type;
        }
      }

      return null;
    }

    public static ColumnType get(Object obj) {
      if (obj != null) {
        Class<?> objClass = obj.getClass();

        for (ColumnType type : ColumnType.values()) {
          for (Class<?> cls : type.getClasses()) {
            if (cls.isAssignableFrom(objClass)) return type;
          }
        }
      }

      return BLOB;
    }

    public static ColumnType get(Object[] elements) {
      return (elements == null || elements.length == 0 ? null : get(elements[0]));
    }

    public static boolean isNumeric(ColumnType colType) {
      switch (colType) {
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
          return true;

        default:
          return false;
      }
    }
  }

  public enum DataFormat {
    SQL, CSV, TSV
  }

}
