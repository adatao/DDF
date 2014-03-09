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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * An implementation of DDF with local memory and local storage
 * 
 */
public class LocalDDF extends DDF {

  private static final long serialVersionUID = -4318701865079177594L;

  protected static final LocalDDFManager sDummyLocalDDFManager = new LocalDDFManager();


  private Class<?> mDataType;
  @Expose private List<?> mData; // only needed during serialization
  @Expose private String mDataTypeName; // only needed during serialization


  public LocalDDF(List<?> rows, String namespace, String name, Schema schema) throws DDFException {
    this((DDFManager) null, (List<?>) rows, namespace, name, schema);
    if (rows != null) mDataType = rows.getClass();
  }

  public LocalDDF(DDFManager manager, List<?> rows, String namespace, String name, Schema schema) throws DDFException {
    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new LocalDDF");
    mDataType = rows.getClass();
    this.initialize(manager, rows, namespace, name, schema);
  }

  /**
   * This signature is needed to support {@link DDFManager#newDDF(DDFManager, Object, Class, String, String, Schema)}
   * 
   * @param manager
   * @param rows
   * @param namespace
   * @param name
   * @param schema
   * @throws DDFException
   */
  public LocalDDF(DDFManager manager, Object rows, String namespace, String name, Schema schema) throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new LocalDDF");
    mDataType = rows.getClass();
    this.initialize(manager, rows, namespace, name, schema);
  }

  /**
   * Signature without List, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   * @throws DDFException 
   */
  public LocalDDF(DDFManager manager) throws DDFException {
    super(manager, sDummyLocalDDFManager);
  }

  /**
   * For serdes only
   * @throws DDFException 
   */
  protected LocalDDF() throws DDFException {
    super(sDummyLocalDDFManager);
  }



  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) {
    return (List<T>) this.getRepresentationHandler().get(rowType);
  }

  public void setList(List<?> data) {
    this.getRepresentationHandler().set(data);
  }



  // //// ISerializable //////


  /**
   * Override to snapshot our List<?> into a local variable for serialization
   */
  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    mData = this.getList(mDataType);
    mDataTypeName = (mDataType != null ? mDataType.getName() : null);
  }

  /**
   * Override to remove our List<?> snapshot from a local variable for serialization
   */
  @Override
  public void afterSerialization() throws DDFException {
    mData = null;
    mDataTypeName = null;
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
      if (!Strings.isNullOrEmpty(mDataTypeName)) {
        mDataType = Class.forName(mDataTypeName);
      }

      if (mData != null) {
        this.setList(mData);

        // See if we need to "unwrap" this object and return the wrapped object instead
        JsonElement deserializedWrappedObject = (serializationData instanceof JsonObject ? //
        ((JsonObject) serializationData).get("mData")
            : null);
        
        deserializedObject = LocalPersistible.unwrapDeserializedObject(mData, deserializedObject,
            (JsonElement) deserializedWrappedObject);
      }

      return super.afterDeserialization(deserializedObject, serializationData);

    } catch (Exception e) {
      if (e instanceof DDFException) throw (DDFException) e;
      else throw new DDFException(e);
    }
  }
}
