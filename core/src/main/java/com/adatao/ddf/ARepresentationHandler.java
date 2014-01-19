/**
 * 
 */
package com.adatao.ddf;

import java.util.HashMap;

/**
 * @author ctn
 * 
 */
public class ARepresentationHandler extends ADDFFunctionalGroupHandler implements IHandleRepresentations {

  public ARepresentationHandler(ADDFImplementor container) {
    super(container);
  }


  // The various representations for our DDF
  private HashMap<String, Object> mReps = new HashMap<String, Object>();

  protected String getKeyFor(Class<?> containerType, Class<?> elementType) {
    return containerType.toString() + "[" + elementType.toString() + "]";
  }


  /**
   * Gets an existing representation for our {@link DDF} matching the given containerType and
   * elementType, if any.
   * 
   * @param containerType
   *          the type of the container
   * @param elementType
   *          the type of each element in the container
   * 
   * @return null if no matching representation available
   */
  @Override
  public Object get(Class<?> containerType, Class<?> elementType) {
    return mReps.get(getKeyFor(containerType, elementType));
  }

  /**
   * Resets (or clears) all representations
   */
  @Override
  public void reset() {
    mReps.clear();
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   * 
   * @param containerType
   *          the type of the container
   * 
   * @param elementType
   *          the type of each element in the container
   */
  @Override
  public void set(Object data, Class<?> containerType, Class<?> elementType) {
    this.reset();
    this.add(data, containerType, elementType);
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones but
   * replacing the one that matches the given containerType, elementType tuple.
   * 
   * @param containerType
   *          the type of the container
   * 
   * @param elementType
   *          the type of each element in the container
   */
  @Override
  public void add(Object data, Class<?> containerType, Class<?> elementType) {
    mReps.put(getKeyFor(containerType, elementType), data);
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
}
