package com.adatao.ddf.content;

public class Schema {
  private String mTableName;
  private ColumnInfo[] mColumnMetaData;

  public String getmTableName() {
    return mTableName;
  }

  public void setmTableName(String mTableName) {
    this.mTableName = mTableName;
  }

  public ColumnInfo[] getmColumnMetaData() {
    return mColumnMetaData;
  }

  public void setmColumnMetaData(ColumnInfo[] columnMetaData) {
    this.mColumnMetaData = columnMetaData;
  }

  public void setColumnNames(String[] names) {
    int length = names.length < mColumnMetaData.length ? names.length
        : mColumnMetaData.length;
    for (int i = 0; i < length; i++) {
      mColumnMetaData[i].setIndex(i).setName(names[i]);
    }
  }

  public ColumnInfo getColumnInfoByIndex(int i) {
    if (mColumnMetaData == null) {
      return null;
    }
    if (i < 0 || i >= mColumnMetaData.length) {
      return null;
    }

    return mColumnMetaData[i];
  }

  public ColumnInfo getColumnInfoByName(String name) {
    Integer i = getColumnIndexByName(name);
    if (i == null) {
      return null;
    }

    return getColumnInfoByIndex(i);
  }

  public Integer getColumnIndexByName(String name) {
    if (mColumnMetaData == null) {
      return null;
    }
    for (int i = 0; i < mColumnMetaData.length; i++) {
      if (mColumnMetaData[i].getName().equals(name)) {
        return i;
      }
    }
    return null;
  }
}
