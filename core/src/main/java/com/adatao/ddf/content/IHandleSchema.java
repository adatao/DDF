package com.adatao.ddf.content;

import java.util.List;

import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;


public interface IHandleSchema extends IHandleDDFFunctionalGroup {

  public Schema getSchema();

  public void setSchema(Schema schema);

  public String getTableName();

  public List<Column> getColumns();

  public long getNumColumns();

  public String newTableName();

  public int getColumnIndex(String columnName);
}
