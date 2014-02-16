/**
 * 
 */
package com.adatao.ddf.content;

import java.util.List;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.content.Schema.Column;

/**
 * @author ctn
 * 
 */
public class ASchemaHandler extends ADDFFunctionalGroupHandler implements IHandleSchema {

  public ASchemaHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
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
}
