/**
 * 
 */
package com.adatao.ddf.content;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;

/**
 * @author ctn
 * 
 */
public class SchemaHandler extends ADDFFunctionalGroupHandler implements IHandleSchema {

  public SchemaHandler(ADDFManager theContainer) {
    super(theContainer);
  }


  private Schema mSchema;

  public Schema getSchema() {
    return mSchema;
  }

  public void setSchema(Schema theSchema) {
    this.mSchema = theSchema;
  }

}
