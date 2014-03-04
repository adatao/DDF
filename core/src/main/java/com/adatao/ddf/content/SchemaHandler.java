/**
 * 
 */
package com.adatao.ddf.content;


import java.util.List;
import java.util.UUID;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

/**
 * @author ctn
 * 
 */
public class SchemaHandler extends ADDFFunctionalGroupHandler implements IHandleSchema {


  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }


  private Schema mSchema;


  @Override
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public void setSchema(Schema theSchema) {
    this.mSchema = theSchema;
  }

  /**
   * @return the Schema's table name
   */
  @Override
  public String getTableName() {
    return mSchema != null ? mSchema.getTableName() : null;
  }

  @Override
  public List<Column> getColumns() {
    return mSchema != null ? mSchema.getColumns() : null;
  }

  @Override
  public String newTableName() {
    return newTableName(this.getDDF());
  }

  @Override
  public String newTableName(Object forObject) {
    if (forObject == null) forObject = this.getDDF();

    return (this.getDDF() != null) //
    ? String.format("%s-%s-%s", forObject.getClass().getSimpleName(), this.getDDF().getEngine(), UUID.randomUUID()) //
        : String.format("DDF-%s", UUID.randomUUID());
  }

  @Override
  public int getNumColumns() {
    return mSchema != null ? mSchema.getNumColumns() : -1;
  }

  @Override
  public int getColumnIndex(String columnName) {
    return mSchema != null ? mSchema.getColumnIndex(columnName) : -1;
  }

}
