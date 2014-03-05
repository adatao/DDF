/**
 * 
 */
package com.adatao.ddf.content;


import java.util.HashMap;
import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.types.NA;

/**
 * @author ctn
 * 
 */
public class RepresentationHandler extends ADDFFunctionalGroupHandler implements IHandleRepresentations {

  public RepresentationHandler(DDF theDDF) {
    super(theDDF);
  }


  // The various representations for our DDF
  protected HashMap<String, Object> mReps = new HashMap<String, Object>();


  protected String getKeyFor(Class<?> dataType) {
    return this.getSafedataType(dataType).toString();
  }

  protected Class<?> getSafedataType(Class<?> dataType) {
    return dataType != null ? dataType : NA.class;
  }

  /**
   * Gets an existing representation for our {@link DDF} matching the given dataType, if any.
   * 
   * @param dataType
   *          the type of the DDF data representation
   * 
   * @return null if no matching representation available
   */
  @Override
  public Object get(Class<?> dataType) {
    return this.get(dataType, true);
  }

  private Object get(Class<?> dataType, boolean doCreate) {
    dataType = this.getSafedataType(dataType);

    Object obj = mReps.get(getKeyFor(dataType));
    if (obj == null && doCreate) {
      obj = this.createRepresentation(dataType);
      if (obj != null) this.add(obj);
    }

    return obj;
  }


  private Class<?> mDefaultDataType;


  /**
   * Returns the default dataType for this engine. The base implementation returns Object[][].class.
   * 
   * @return
   */
  @Override
  public Class<?> getDefaultDataType() {
    return mDefaultDataType;
  }

  public void setDefaultDataType(Class<?> dataType) {
    mDefaultDataType = dataType;
  }

  /**
   * Returns the default columnType for this engine. The base implementation returns Object.class.
   * 
   * @return
   */
  public Class<?> getDefaultColumnType() {
    return Object.class;
  }

  @Override
  public Object getDefault() {
    return this.get(this.getDefaultDataType());
  }

  /**
   * Resets (or clears) all representations
   */
  @Override
  public void reset() {
    mReps.clear();
    this.setDefaultDataType(null);
  }

  /**
   * Converts from existing representation(s) to the desired representation, which has the specified dataType.
   * 
   * The base representation returns only the default representation if the dataType matches the default type. Otherwise
   * it returns null.
   * 
   * @param dataType
   * @return
   */
  public Object createRepresentation(Class<?> dataType) {
    return (this.getDefaultDataType() != null && this.getDefaultDataType().equals(dataType)) ? this
        .get(dataType, false) : null;
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   * 
   */
  @Override
  public void set(Object data) {
    this.reset();
    if (data != null) this.setDefaultDataType(data.getClass());
    this.add(data);
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones but replacing the one that
   * matches the given DDFManagerType, dataType tuple.
   */
  @Override
  public void add(Object data) {
    if (data == null) return;
    if (this.getDefaultDataType() == null) this.setDefaultDataType(data.getClass());
    mReps.put(getKeyFor(data.getClass()), data);
  }

  /**
   * Removes a representation from the set of existing representations.
   * 
   * @param dataType
   */
  @Override
  public void remove(Class<?> dataType) {
    mReps.remove(getKeyFor(dataType));
  }

  /**
   * Returns a String list of current representations, useful for debugging
   */
  public String getList() {
    String result = "";
    int i = 1;

    for (String s : mReps.keySet()) {
      result += (i++) + ". key='" + s + "', value='" + mReps.get(s) + "'\n";
    }

    return result;
  }

  @Override
  public void cleanup() {
    mReps.clear();
    super.cleanup();
    uncacheAll();
  }

  @Override
  public void cacheAll() {
    // TODO Auto-generated method stub

  }

  @Override
  public void uncacheAll() {
    // TODO Auto-generated method stub

  }


  public enum RepresentationType {
    DEFAULT_TYPE, ARRAY_OBJECT, ARRAY_DOUBLE, ARRAY_LABELEDPOINT;

    public static RepresentationType fromString(String s) {
      if (s == null || s.length() == 0) return null;
      s = s.toUpperCase().trim();
      for (RepresentationType t : values()) {
        if (s.equals(t.name())) return t;
      }
      return null;
    }
  }
}
