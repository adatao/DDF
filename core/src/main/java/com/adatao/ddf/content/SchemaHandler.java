/**
 * 
 */
package com.adatao.ddf.content;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.util.DDFUtils;

/**
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
  public String newTableName(Object obj) {
    return DDFUtils.generateObjectName(obj);
  }

  @Override
  public int getNumColumns() {
    return mSchema != null ? mSchema.getNumColumns() : -1;
  }

  @Override
  public int getColumnIndex(String columnName) {
    return mSchema != null ? mSchema.getColumnIndex(columnName) : -1;
  }

  @Override
  public Schema generateSchema() {
    if (this.getSchema() != null) return this.getSchema();

    // Try to infer from the DDF's data
    Object data = this.getDDF().getRepresentationHandler().getDefault();

    // TODO: for now, we'll just support the "null" case
    if (data == null) return new Schema(null, "null BLOB");

    return null;
  }

}
