package com.adatao.ddf.content;

import java.util.List;

import com.adatao.ddf.content.Schema.Column;


public interface IHandleSchema {

  public Schema getSchema();

  public void setSchema(Schema schema);

  public String getTableName();
  
  public List<Column> getColumns();

}
