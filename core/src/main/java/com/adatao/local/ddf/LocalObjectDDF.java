/**
 * 
 */
package com.adatao.local.ddf;


import java.util.ArrayList;
import java.util.List;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.Config.ConfigConstant;
import com.adatao.ddf.types.IGloballyAddressable;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * An implementation of DDF that backs a single object, with local memory and local storage. It uses JSON serialization
 * when persisting that object's representation to/from storage.
 * 
 * This class is parameterizable since we only support one representation of type T.
 */
public class LocalObjectDDF<T> extends LocalDDF {

  private static final long serialVersionUID = 8212580667722563775L;

  // This is not serializable, so we need to get it from mObjectClassName when deserializing
  private Class<T> mObjectClass;

  // For serdes only
  @Expose private T mObject;

  // For serdes only
  @Expose private String mObjectClassName;


  /**
   * Given an object of type T, we construct this DDF as a List<T> containing only one element, the object itself. The
   * schema used will be {name=object, type=blob}.
   * 
   * @param manager
   * @param object
   * @param namespace
   * @param name
   * @throws DDFException
   */
  public LocalObjectDDF(DDFManager manager, T object, String namespace, String name) throws DDFException {
    super(manager);
    this.initialize(manager, object, namespace, name);
  }

  /**
   * Signature without List, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   * @throws DDFException
   */
  public LocalObjectDDF(T object) throws DDFException {
    this(DDFManager.get(ConfigConstant.ENGINE_NAME_LOCAL.toString()), object, null, null);
  }

  @SuppressWarnings("unchecked")
  protected void initialize(DDFManager manager, T object, String namespace, String name) throws DDFException {
    if (object == null) throw new DDFException("Non-null object is required to instantiate a new LocalObjectDDF");

    List<T> rows = new ArrayList<T>();
    rows.add(object);
    this.setObjectClass((Class<T>) object.getClass());

    if (Strings.isNullOrEmpty(namespace) && object instanceof IGloballyAddressable) {
      this.setNamespace(((IGloballyAddressable) object).getNamespace());
    }

    if (Strings.isNullOrEmpty(name)) {
      if (object instanceof IGloballyAddressable) name = ((IGloballyAddressable) object).getName();
      else name = this.getSchemaHandler().newTableName();
    }

    this.initialize(manager, rows, this.getObjectClass(), namespace, name, new Schema(name, "object blob"));
  }

  /**
   * @return the mObjectClass
   * @throws DDFException
   */
  @SuppressWarnings("unchecked")
  public Class<T> getObjectClass() throws DDFException {
    if (mObjectClass == null && mObjectClassName != null) try {
      mObjectClass = (Class<T>) Class.forName(mObjectClassName);

    } catch (ClassNotFoundException e) {
      throw new DDFException(e);
    }

    return mObjectClass;
  }

  /**
   * @param mObjectClass
   *          the mObjectClass to set
   */
  private void setObjectClass(Class<T> objectClass) {
    this.mObjectClass = objectClass;
    this.mObjectClassName = objectClass.getName();
  }



  @SuppressWarnings("unchecked")
  public T getObject() throws DDFException {
    // First try to get the object from the official place, which is from our List
    // If it's not there, then try to use mObject instead.
    List<T> list = this.getList(this.getObjectClass());
    T gotObject = (list != null && !list.isEmpty() ? list.get(0) : null);
    if (gotObject != null) mObject = gotObject;
    else this.setObject(mObject);

    // Update the objectClass for serdes
    if (mObject != null) this.setObjectClass((Class<T>) mObject.getClass());
    return mObject;
  }

  @SuppressWarnings("unchecked")
  public void setObject(T object) throws DDFException {
    if (object == null) throw new DDFException("Cannot set to null object");

    // Immediately set mObject for serdes
    mObject = object;

    // Now update our List
    List<T> list = this.getList(this.getObjectClass());
    if (list == null) {
      list = new ArrayList<T>();
      this.getRepresentationHandler().set(list, this.getObjectClass());
    }

    list.clear();
    list.add(object);
    this.setObjectClass((Class<T>) object.getClass());
  }


  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    this.getObject(); // to trigger the setting of the mObject field
  }

  @Override
  public void afterDeserialization(Object data) throws DDFException {
    // We need to manually deserialize mObject, and create our own representation for it
    if (data instanceof JsonObject) {
      JsonObject jObj = (JsonObject) data;
      JsonElement mObjectElement = jObj.get("mObject");
      if (mObjectElement != null) {
        mObject = new Gson().fromJson(mObjectElement, this.getObjectClass());
      }

      this.setObject(mObject);
    }

    this.initialize(this.getManager(), this.getObject(), this.getNamespace(), this.getName());
    super.afterDeserialization(data);
  }
}
