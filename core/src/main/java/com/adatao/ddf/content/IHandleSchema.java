package com.adatao.ddf.content;


import java.util.List;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;


public interface IHandleSchema extends IHandleDDFFunctionalGroup {

  public Schema getSchema();

  public void setSchema(Schema schema);

  public String getTableName();

  public List<Column> getColumns();

  public int getNumColumns();

  public String newTableName();

  /**
   * The name will reflect the class name of the given forObject
   * 
   * @param forObject
   * @return
   */
  public String newTableName(Object forObject);

  public int getColumnIndex(String columnName);
}
