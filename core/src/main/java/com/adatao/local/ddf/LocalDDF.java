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
 * An implementation of DDF with local memory and local storage
 * 
 */
public class LocalDDF extends DDF {

  public <T> LocalDDF(DDFManager manager, List<T> rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager);
    if (rows == null) throw new DDFException("Non-null List is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }

  public <T> LocalDDF(DDFManager manager, Object rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager);
    if (rows == null) throw new DDFException("Non-null List is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }

  /**
   * Signature without List, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   */
  public LocalDDF(DDFManager manager) {
    super(manager);
  }


  public LocalDDF() {
    super(null);
  }


  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) throws DDFException {
    Object obj = this.getRepresentationHandler().get(rowType);
    if (obj instanceof List<?>) return (List<T>) obj;
    else throw new DDFException("Unable to get List with element type " + rowType);
  }
}
