/**
 * 
 */
package com.adatao.ddf.content;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.WeakHashMap;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFHelper;
import com.adatao.ddf.DDF;

/**
 * @author ctn
 * 
 */
public abstract class AViewHandler extends ADDFFunctionalGroupHandler implements IHandleViews {

  // A list of views we point to, keyed by the String representation of the view properties
  // such as columns and format
  protected WeakHashMap<String, DDF> mViews = new WeakHashMap<String, DDF>();

  public AViewHandler(ADDFHelper theContainer) {
    super(theContainer);
  }

  /**
   * Converts a view specification into a unique String representation. Note that this is not
   * suitable for very large number of columns. If and when that's needed, provide another method.
   * 
   * @param columns
   * @param format
   * @return
   */
  protected String getKeyFor(int[] columns, String format) {
    return (Arrays.toString(columns) + format);
  }

  /**
   * Must be implemented by a child class to extract the actual view.
   * 
   * @param columns
   * @param format
   */
  protected abstract DDF getImpl(int[] columns, String format);


  /**
   * Same as {@link #get(int[], int)}, but accepts a java.lang.Enum for format instead.
   * 
   * @param columns
   * @param formatEnum
   *          a java.lang.Enum<?> that will be converted to a String by calling formatEnum.name()
   * @return
   */
  public DDF get(int[] columns, Enum<?> formatEnum) {
    return this.get(columns, (formatEnum == null ? "null" : formatEnum.name()));
  }

  @Override
  public DDF get(int[] columns, String format) {
    String key = this.getKeyFor(columns, format);
    DDF view = mViews.get(key);

    if (view == null) {
      view = this.getImpl(columns, format);
    }

    if (view != null) {
      view.cache();
      mViews.put(key, view);
    }

    return view;
  }

  @Override
  public void reset() {
    mViews.clear();
  }

  @Override
  public void remove(UUID ddfId) {
    if (ddfId == null) return;

    for (Entry<String, DDF> entry : mViews.entrySet()) {
      DDF ddf = entry.getValue();
      if (ddf != null) {
        if (ddfId.equals(ddf.getHelper().getMetaDataHandler().getId())) {
          mViews.remove(entry.getKey());
        }
      }
    }
  }
}
