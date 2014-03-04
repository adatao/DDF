/**
 * 
 */
package com.adatao.local.ddf;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.ISerializable;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.local.ddf.content.PersistenceHandler.LocalPersistible;
import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;

/**
 * An implementation of DDF with local memory and local storage
 * 
 */
public class LocalDDF extends DDF {

  private static final long serialVersionUID = -4318701865079177594L;

  protected static final LocalDDFManager sDummyLocalDDFManager = new LocalDDFManager();


  private Class<?> mRowType;


  public LocalDDF(List<?> rows, Class<?> rowType, String namespace, String name, Schema schema) throws DDFException {
    this((DDFManager) null, (List<?>) rows, (Class<?>) rowType, namespace, name, schema);
    mRowType = rowType;
  }

  public LocalDDF(DDFManager manager, List<?> rows, Class<?> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new LocalDDF");

    mRowType = rowType;
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
  public LocalDDF(DDFManager manager, Object rows, Class<?> rowType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new LocalDDF");

    mRowType = rowType;
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

  /**
   * For serdes only
   */
  protected LocalDDF() {
    super(sDummyLocalDDFManager);
  }



  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) {
    return (List<T>) this.getRepresentationHandler().get(rowType);
  }

  public void setList(List<?> data, Class<?> rowType) {
    this.getRepresentationHandler().set(data, rowType);
  }



  // //// ISerializable //////

  @Expose private List<?> mData;
  @Expose private String mRowTypeName;


  /**
   * Override to snapshot our List<?> into a local variable for serialization
   */
  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    mData = this.getList(mRowType);
    mRowTypeName = (mRowType != null ? mRowType.getName() : null);
  }

  /**
   * Override to remove our List<?> snapshot from a local variable for serialization
   */
  @Override
  public void afterSerialization() throws DDFException {
    mData = null;
    mRowTypeName = null;
    super.afterSerialization();
  }


  /**
   * Special case: if we hold a single object of type IPersistible, then some magic happens: we will return *that*
   * object as a result of the deserialization, instead of this DDF itself. This makes it possible for clients to do
   * things like<br/>
   * <code>
   *   PersistenceUri uri = model.persist();
   *   Model model = (Model) ddfManager.load(uri);
   * </code> instead of having to do this:<br/>
   * <code>
   *   PersistenceUri uri = model.persist();
   *   LocalDDF ddf = (LocalDDF) ddfManager.load(uri);
   *   Model model = (Model) ddf.getList().get(0);
   * </code>
   */
  @Override
  public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData)
      throws DDFException {

    try {
      if (!Strings.isNullOrEmpty(mRowTypeName)) {
        mRowType = Class.forName(mRowTypeName);
      }

      if (mData != null) {
        this.setList(mData, mRowType);
        deserializedObject = LocalPersistible.parseDeserializedObject(mData, mRowType, deserializedObject);
      }

      return super.afterDeserialization(deserializedObject, serializationData);

    } catch (Exception e) {
      throw new DDFException(e);
    }
  }
}
