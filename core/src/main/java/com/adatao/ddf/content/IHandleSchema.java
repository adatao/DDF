package com.adatao.ddf.content;


import java.util.List;
import com.adatao.ddf.Factor;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;


public interface IHandleSchema extends IHandleDDFFunctionalGroup {

  Schema getSchema();

  void setSchema(Schema schema);

  String getTableName();
  
  Column getColumn(String columnName);

  List<Column> getColumns();

  int getNumColumns();

  String newTableName();

  /**
   * The name will reflect the class name of the given forObject
   * 
   * @param forObject
   * @return
   */
  String newTableName(Object forObject);

  int getColumnIndex(String columnName);

  String getColumnName(int columnIndex);

  /**
   * Generate a basic schema for the current DDF
   */
  Schema generateSchema();

  Factor<?> setAsFactor(String columnName);

  Factor<?> setAsFactor(int columnIndex);

  void unsetAsFactor(String columnName);

  void unsetAsFactor(int columnIndex);
}
