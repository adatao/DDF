/**
 * 
 */
package com.adatao.ddf.content;

import java.util.List;
import java.util.UUID;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.Column;

/**
 * @author ctn
 * 
 */
public class SchemaHandler extends ADDFFunctionalGroupHandler implements IHandleSchema {


  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }

  private Schema mSchema;

  public Schema getSchema() {
    return mSchema;
  }

  public void setSchema(Schema theSchema) {
    this.mSchema = theSchema;
  }

  public String getTableName() {
    return this.mSchema.getTableName();
  }

  public List<Column> getColumns() {
    return this.mSchema.getColumns();
  }

  public String newTableName() {
    return String.format("ddf-%s", UUID.randomUUID());
  }

  public long getNumColumns() {
    return this.mSchema.getNumColumns();
  }

  @Override
  public int getColumnIndex(String columnName) {
    return this.getSchema().getColumnIndex(columnName);
  }

}
