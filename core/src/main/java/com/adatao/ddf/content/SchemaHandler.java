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


  public Schema getSchema() {
    return mSchema;
  }

  public void setSchema(Schema theSchema) {
    this.mSchema = theSchema;
  }

  /**
   * @return the Schema's table name
   */
  public String getTableName() {
    return mSchema != null ? mSchema.getTableName() : null;
  }

  public List<Column> getColumns() {
    return mSchema != null ? mSchema.getColumns() : null;
  }

  public String newTableName() {
    return (this.getDDF() != null) //
    ? String.format("%s-%s-%s", this.getDDF().getClass().getSimpleName(), this.getDDF().getEngine(), UUID.randomUUID()) //
        : String.format("DDF-%s", UUID.randomUUID());
  }

  public long getNumColumns() {
    return mSchema != null ? mSchema.getNumColumns() : -1;
  }

  @Override
  public int getColumnIndex(String columnName) {
    return mSchema != null ? mSchema.getColumnIndex(columnName) : -1;
  }

}
