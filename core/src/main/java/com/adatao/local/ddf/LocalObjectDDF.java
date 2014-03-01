/**
 * 
 */
package com.adatao.local.ddf;


import java.util.ArrayList;
import java.util.List;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.google.gson.annotations.Expose;

/**
 * An implementation of DDF that backs a single object, with local memory and local storage. It uses JSON serialization
 * when persisting that object's representation to/from storage.
 * 
 * This class is parameterizable since we only support one representation of type T.
 */
public class LocalObjectDDF<T> extends LocalDDF {

  private static final long serialVersionUID = 1L;

  // @formatter:off
  /* This is some serious voodoo we want to keep around in case it's useful.
   * http://stackoverflow.com/questions/1901164/get-type-of-a-generic-parameter-in-java-with-reflection
  @SuppressWarnings("unchecked")
  protected final Class<T> mRowType = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  */
  // @formatter:on

  private Class<T> mObjectClass;

  // For serdes only
  @Expose private T mObject;

  // For serdes only
  @SuppressWarnings("unused") @Expose private String mObjectClassName;


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
  @SuppressWarnings("unchecked")
  public LocalObjectDDF(DDFManager manager, T object, String namespace, String name) throws DDFException {
    super(manager);
    if (object == null) throw new DDFException("Non-null object is required to instantiate a new LocalObjectDDF");

    List<T> rows = new ArrayList<T>();
    rows.add(object);
    this.setObjectClass((Class<T>) object.getClass());

    if (name == null) name = this.getSchemaHandler().newTableName();

    this.initialize(manager, rows, this.getObjectClass(), namespace, name, new Schema(name, "object blob"));
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



  /**
   * @return the mObjectClass
   */
  public Class<T> getObjectClass() {
    return mObjectClass;
  }

  /**
   * @param mObjectClass
   *          the mObjectClass to set
   */
  private void setObjectClass(Class<T> objectClass) {
    this.mObjectClass = objectClass;
    this.mObjectClassName = objectClass.getCanonicalName();
  }



  @SuppressWarnings("unchecked")
  public T getObject() throws DDFException {
    List<T> list = this.getList(this.getObjectClass());
    mObject = (list != null && !list.isEmpty() ? list.get(0) : null);
    this.setObjectClass((Class<T>) mObject.getClass());
    return mObject;
  }

  public void setObject(T object) throws DDFException {
    List<T> list = this.getList(this.getObjectClass());
    if (list == null) throw new DDFException("List<T> representation cannot be null");
    list.clear();
    list.add(object);
  }


  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    this.getObject(); // to trigger the setting of the mObject field
  }
}
