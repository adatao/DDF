/**
 * 
 */
package com.adatao.ddf.content;


import java.util.HashMap;
import java.util.List;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.misc.Config;
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


  protected String getKeyFor(Class<?> containerType, Class<?> unitType) {
    return String.format("%s_%s", containerType.toString(), unitType.toString());
  }

  protected Class<?> getSafedataType(Class<?> dataType) {
    return dataType != null ? dataType : NA.class;
  }

  /**
   * Gets an existing representation for our {@link DDF} matching the given dataType, if any.
   * 
   * @param containerType
   *          the type of the DDF data representation
   * @param unitType
   * 
   * @return null if no matching representation available
   */
  @Override
  public Object get(Class<?> containerType, Class<?> unitType) {
    return this.get(containerType, unitType, true);
  }

  @Override
  public Object get(Class<?> unitType) throws DDFException {
    return this.get(this.getDefaultEngineContainerType(), unitType);
  }

  private Object get(Class<?> containerType, Class<?> unitType, boolean doCreate) {
    containerType = this.getSafedataType(containerType);
    unitType = this.getSafedataType(unitType);

    Object obj = mReps.get(getKeyFor(containerType, unitType));

    if (obj == null && doCreate) {
      obj = this.createRepresentation(containerType, unitType);
      if (obj != null) this.add(obj, containerType, unitType);
    }

    return obj;
  }


  private Class<?> mDefaultUnitType;

  private Class<?> mDefaultContainerType;
  /**
   * Returns the default dataType for this engine. The base implementation returns Object[][].class.
   * 
   * @return
   */
  @Override
  public Class<?> getDefaultUnitType() {
    return mDefaultUnitType;
  }

  public void setDefaultUnitType(Class<?> unitType) {
    mDefaultUnitType = unitType;
  }

  @Override
  public Class<?> getDefaultContainerType() {
    return mDefaultContainerType;
  }

  private Class<?> getDefaultEngineContainerType() throws DDFException{
    try {
      String containerName = Config.getValueWithGlobalDefault(this.getEngine(), "DefaultEngineContainerType");
      return Class.forName(containerName);
    } catch(Exception e) {
      throw new DDFException("Cannot get engine's default ContainerType");
    }
  }
  public void setDefaultContainerType(Class<?> containerType) {
    mDefaultContainerType = containerType;
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
    return this.get(this.getDefaultContainerType(), getDefaultUnitType());
  }

  /**
   * Resets (or clears) all representations
   */
  @Override
  public void reset() {
    mReps.clear();
    this.setDefaultUnitType(null);
  }

  /**
   * Converts from existing representation(s) to the desired representation, which has the specified dataType.
   * 
   * The base representation returns only the default representation if the dataType matches the default type. Otherwise
   * it returns null.
   * 
   * @param containerType
   * @return
   */
  public Object createRepresentation(Class<?> containerType, Class<?> unitType) {
    return (this.getDefaultContainerType() != null && this.getDefaultContainerType().equals(containerType) ||
            this.getDefaultUnitType() != null && this.getDefaultUnitType().equals(unitType)) ? this
        .get(containerType, unitType, false) : null;
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   * 
   */
  @Override
  public void set(Object data, Class<?> containerType, Class<?> unitType) {
    this.reset();
    if (data != null) {
      this.setDefaultContainerType(containerType);
      this.setDefaultUnitType(unitType);
    }
    this.add(data, containerType, unitType);
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones but replacing the one that
   * matches the given DDFManagerType, containerType, unitType.
   */
  @Override
  public void add(Object data, Class<?> containerType, Class<?> unitType) {
    if (data == null) return;
    if (this.getDefaultContainerType() == null || this.getDefaultUnitType() == null) {
      this.setDefaultContainerType(containerType);
      this.setDefaultUnitType(unitType);
    }
    mReps.put(getKeyFor(containerType, unitType), data);
  }

  /**
   * Removes a representation from the set of existing representations.
   * 
   * @param containerType
   * @param unitType
   */
  @Override
  public void remove(Class<?> containerType, Class<?> unitType) {
    mReps.remove(getKeyFor(containerType, unitType));
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


  public enum KnownTypes {
    DEFAULT_TYPE, ARRAY_OBJECT, ARRAY_DOUBLE, ARRAY_LABELEDPOINT;

    public static KnownTypes fromString(String s) {
      if (s == null || s.length() == 0) return null;
      s = s.toUpperCase().trim();
      for (KnownTypes t : values()) {
        if (s.equals(t.name())) return t;
      }
      return null;
    }
  }
}
