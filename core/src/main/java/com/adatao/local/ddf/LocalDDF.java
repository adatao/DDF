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


  private Class<?> mUnitType;
  @Expose private List<?> mData; // only needed during serialization
  @Expose private String mUnitTypeName; // only needed during serialization


  public LocalDDF(List<?> rows, Class<?> unitType, String namespace, String name, Schema schema) throws DDFException {
    this((DDFManager) null, (List<?>) rows, unitType, namespace, name, schema);
    if (rows != null) mUnitType = unitType;
  }

  public LocalDDF(DDFManager manager, List<?> rows, Class<?> unitType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new LocalDDF");
    mUnitType = unitType;
    this.initialize(manager, rows, new Class[] { List.class, unitType }, namespace, name, schema);
  }

  /**
   * This signature is needed to support {@link DDFManager#newDDF(DDFManager, Object, Class[], String, String, Schema)}
   * 
   * @param manager
   * @param rows
   * @param typeSpecs
   * @param namespace
   * @param name
   * @param schema
   * @throws DDFException
   */
  public LocalDDF(DDFManager manager, Object rows, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager, sDummyLocalDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new LocalDDF");
    mUnitType = (typeSpecs != null && typeSpecs.length > 0) ? typeSpecs[1] : null;
    this.initialize(manager, rows, typeSpecs, namespace, name, schema);
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
   * 
   * @throws DDFException
   */
  protected LocalDDF() throws DDFException {
    super(sDummyLocalDDFManager);
  }



  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) {
    return (List<T>) this.getRepresentationHandler().get(List.class, rowType);
  }

  public void setList(List<?> data, Class<?> rowType) {
    this.getRepresentationHandler().set(data, List.class, rowType);
  }



  // //// ISerializable //////


  /**
   * Override to snapshot our List<?> into a local variable for serialization
   */
  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    mData = this.getList(mUnitType);
    mUnitTypeName = (mUnitType != null ? mUnitType.getName() : null);
  }

  /**
   * Override to remove our List<?> snapshot from a local variable for serialization
   */
  @Override
  public void afterSerialization() throws DDFException {
    mData = null;
    mUnitType = null;
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
      if (!Strings.isNullOrEmpty(mUnitTypeName)) {
        mUnitType = Class.forName(mUnitTypeName);
      }

      if (mData != null) {
        this.setList(mData, mUnitType);

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
