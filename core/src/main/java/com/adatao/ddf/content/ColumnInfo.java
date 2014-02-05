package com.adatao.ddf.content;

public class ColumnInfo {
  String header = null;
  String type;
  int columnIndex = -1;

  public ColumnInfo(String header, String type) {
    this.header = header;
    this.type = type;
  }

  public ColumnInfo(String header, String type, int columnIndex) {
    this(header, type);
    this.columnIndex = columnIndex;
  }

  public String getHeader() {
    return header;
  }

  public ColumnInfo setHeader(String header) {
    this.header = header;
    return this;
  }

  public String getType() {
    return type;
  }

  public ColumnInfo setType(String type) {
    this.type = type;
    return this;
  }

  public int getColumnIndex() {
    return this.columnIndex;
  }

  public ColumnInfo setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
    return this;
  }

}
