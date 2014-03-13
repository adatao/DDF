/**
 * 
 */
package com.adatao.basic.ddf;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

/**
 * An implementation of DDFManager with local memory and local storage
 */
public class BasicDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "basic";
  }



  public BasicDDFManager() {}


  public <T> DDF newDDF(List<T> rows, Class<T> unitType, String namespace, String name, Schema schema)
      throws DDFException {

    if (rows == null || rows.size() == 0) {
      throw new DDFException("Non-null/zero-length List is required to instantiate a new BasicDDF");
    }

    return this.newDDF(this, rows, new Class[] { List.class, unitType }, namespace, name, schema);
  }
}
