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

  private static final long serialVersionUID = 1L;

  protected static final LocalDDFManager sDummyLocalDDFManager = new LocalDDFManager();


  public <T> LocalDDF(DDFManager manager, List<T> rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }

  // @formatter:off
  /* [ctn] Wondering out loud: do we need to support anything other than List<T> rows?
  @Deprecated
  public <T> LocalDDF(DDFManager manager, Object rows, Class<T> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new LocalDDF");
    this.initialize(manager, rows, rowType, namespace, name, schema);
  }
  */
  // @formatter:on

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
  public <T> List<T> getList(Class<T> rowType) throws DDFException {
    Object obj = this.getRepresentationHandler().get(rowType);
    if (obj instanceof List<?>) return (List<T>) obj;
    else throw new DDFException("Unable to get List with element type " + rowType);
  }

}
