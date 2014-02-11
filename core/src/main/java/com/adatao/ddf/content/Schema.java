package com.adatao.ddf.content;

import java.util.List;

/**
 * Table schema of a DDF including table name and column metadata
 * 
 * @author bhan
 * 
 */
enum ColumnType {
  STRING, DOUBLE, INT
}

public class Schema {
  private String mTableName;
  private List<ColumnInfo> mColumnMetaData;

  public Schema(String tableName, List<ColumnInfo> columnMetaData) {
    this.mTableName = tableName;
    this.mColumnMetaData = columnMetaData;
  }

  public String getTableName() {
    return mTableName;
  }

  public void setTableName(String mTableName) {
    this.mTableName = mTableName;
  }

  public List<ColumnInfo> getColumnMetaData() {
    return mColumnMetaData;
  }

  public void setColumnMetaData(List<ColumnInfo> columnMetaData) {
    this.mColumnMetaData = columnMetaData;
  }

  public void setColumnNames(List<String> names) {
    int length = names.size() < mColumnMetaData.size() ? names.size()
        : mColumnMetaData.size();
    for (int i = 0; i < length; i++) {
      mColumnMetaData.get(i).setName(names.get(i));
    }
  }

  public ColumnInfo getColumn(int i) {
    if (mColumnMetaData == null) {
      return null;
    }
    if (i < 0 || i >= mColumnMetaData.size()) {
      return null;
    }

    return mColumnMetaData.get(i);
  }

  public ColumnInfo getColumn(String name) {
    Integer i = getColumnIndex(name);
    if (i == null) {
      return null;
    }

    return getColumn(i);
  }

  public Integer getColumnIndex(String name) {
    if (mColumnMetaData == null) {
      return null;
    }
    for (int i = 0; i < mColumnMetaData.size(); i++) {
      if (mColumnMetaData.get(i).getName().equals(name)) {
        return i;
      }
    }
    return null;
  }

  public long getNumColumns() {
    return this.mColumnMetaData.size();
  }

  public static class ColumnInfo {
    private String mName = null;
    private ColumnType mType;

    public ColumnInfo(String header, ColumnType type) {
      this.mName = header;
      this.mType = type;
    }

    public String getName() {
      return mName;
    }

    public ColumnInfo setName(String name) {
      this.mName = name;
      return this;
    }

    public ColumnType getType() {
      return mType;
    }

    public ColumnInfo setType(ColumnType type) {
      this.mType = type;
      return this;
    }

  }

}
