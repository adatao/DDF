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

  private static final long serialVersionUID = -4318701865079177594L;

  protected static final LocalDDFManager sDummyLocalDDFManager = new LocalDDFManager();


  public <T> LocalDDF(DDFManager manager, List<T> rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }

  /**
   * This signature is needed to support {@link DDFManager#newDDF(DDFManager, Object, Class, String, String, Schema)}
   * 
   * @param manager
   * @param rows
   * @param rowType
   * @param namespace
   * @param name
   * @param schema
   * @throws DDFException
   */
  public <T> LocalDDF(DDFManager manager, Object rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }

  /**
   * Signature without List, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   */
  public LocalDDF(DDFManager manager) {
    super(manager, sDummyLocalDDFManager);
  }

  public LocalDDF() {
    super(sDummyLocalDDFManager);
  }



  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) {
    return (List<T>) this.getRepresentationHandler().get(rowType);
  }

}
