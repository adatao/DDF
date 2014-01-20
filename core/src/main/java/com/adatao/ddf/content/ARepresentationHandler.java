/**
 * 
 */
package com.adatao.ddf.content;

import java.util.HashMap;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFHelper;
import com.adatao.ddf.DDF;

/**
 * @author ctn
 * 
 */
public abstract class ARepresentationHandler extends ADDFFunctionalGroupHandler implements IHandleRepresentations {

  public ARepresentationHandler(ADDFHelper container) {
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
   * Removes a representation from the set of existing representations.
   * 
   * @param containerType
   * @param elementType
   */
  @Override
  public void remove(Class<?> containerType, Class<?> elementType) {
    mReps.remove(getKeyFor(containerType, elementType));
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
