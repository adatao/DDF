/**
 * 
 */
package com.adatao.ddf;

import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * Abstract base class for a handler of a group of DDF functionality, e.g., {@link IHandleMetadata},
 * or {@link IHandleViews}.
 * 
 * @author ctn
 * 
 */
public abstract class ADDFFunctionalGroupHandler extends ALoggable implements IHandleDDFFunctionalGroup,
    ISupportPhantomReference {

  public ADDFFunctionalGroupHandler(DDF theDDF) {
    this.setDDF(theDDF);
    PhantomReference.register(this);
  }

  private DDF mDDF;

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.IHandleDDFFunctionalGroup#getDDF()
   */
  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public DDFManager getManager() {
    return this.getDDF().getManager();
  }


  @Override
  // ISupportPhantomReference
  public void cleanup() {
    this.setDDF(null);
  }
}
