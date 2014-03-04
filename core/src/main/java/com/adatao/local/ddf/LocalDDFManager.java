/**
 * 
 */
package com.adatao.local.ddf;

import java.util.List;

import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

/**
 * An implementation of DDFManager with local memory and local storage
 */
public class LocalDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "local";
  }



  public LocalDDFManager() {
  }


  public <T> DDF newDDF(List<T> rows, String namespace, String name, Schema schema) throws DDFException {
    if (rows == null || rows.size() == 0) throw new DDFException(
        "Non-null/zero-length List is required to instantiate a new LocalDDF");

    Class<?> rowType = rows.get(0).getClass();
    return this.newDDF(this, rows, rowType, namespace, name, schema);
  }
}
