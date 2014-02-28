package com.adatao.ddf.content;

import java.util.List;

import com.adatao.ddf.IHandleDDFFunctionalGroup;
import com.adatao.ddf.content.Schema.Column;


public interface IHandleSchema extends IHandleDDFFunctionalGroup {

  public Schema getSchema();

  public void setSchema(Schema schema);

  public String getTableName();

  public List<Column> getColumns();

  public long getNumColumns();

  public String newTableName();

  public int getColumnIndex(String columnName);
}
