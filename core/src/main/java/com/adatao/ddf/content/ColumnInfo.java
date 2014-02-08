package com.adatao.ddf.content;

public class ColumnInfo {
  private String mName = null;
  private String mType;
  int mIndex = -1;

  public ColumnInfo(String header, String type) {
    this.mName = header;
    this.mType = type;
  }

  public ColumnInfo(String name, String type, int index) {
    this(name, type);
    this.mIndex = index;
  }

  public String getName() {
    return mName;
  }

  public ColumnInfo setName(String name) {
    this.mName = name;
    return this;
  }

  public String getType() {
    return mType;
  }

  public ColumnInfo setType(String type) {
    this.mType = type;
    return this;
  }

  public int getIndex() {
    return this.mIndex;
  }

  public ColumnInfo setIndex(int index) {
    this.mIndex = index;
    return this;
  }

}
