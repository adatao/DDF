/**
 * 
 */
package com.adatao.ddf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * Abstract base class for a handler of a group of DDF functionality, e.g., {@link IHandleMetadata},
 * or {@link IHandleViews}.
 * 
 * @author ctn
 * 
 */
public abstract class ADDFFunctionalGroupHandler implements ISupportPhantomReference {
  protected Logger LOG = LoggerFactory.getLogger(this.getClass());

  public ADDFFunctionalGroupHandler(ADDFManager theDDFManager) {
    this.setManager(theDDFManager);

    PhantomReference.register(this);
  }


  /**
   * @return the {@link DDF} this handler handles, via the DDFManager
   */
  public DDF getDDF() {
    return this.getManager().getDDF();
  }

  private ADDFManager mDDFManager;

  /**
   * @return the {@link ADDFManager} that contains this handler
   */
  public ADDFManager getManager() {
    return mDDFManager;
  }

  /**
   * @param aDDFManager
   *          the containing {@link ADDFManager} to set
   * 
   * @return this instance, for call-chaining style
   */
  public ADDFFunctionalGroupHandler setManager(ADDFManager aDDFManager) {
    this.mDDFManager = aDDFManager;
    return this;
  }

  @Override
  // ISupportPhantomReference
  public void cleanup() {
    this.setManager(null);
  }
}
