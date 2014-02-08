/**
 * 
 */
package com.adatao.ddf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.content.IHandleMetadata;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * Abstract base class for a handler of a group of DDF functionality, e.g., {@link IHandleMetadata},
 * or {@link IHandleFilteringAndProjections}.
 * 
 * @author ctn
 * 
 */
public abstract class ADDFFunctionalGroupHandler implements ISupportPhantomReference {
  protected Logger LOG = LoggerFactory.getLogger(this.getClass());

  public ADDFFunctionalGroupHandler(ADDFHelper theContainer) {
    this.setContainer(theContainer);

    PhantomReference.register(this);
  }


  /**
   * @return the {@link DDF} this handler handles, via the Container
   */
  public DDF getDDF() {
    return this.getContainer().getDDF();
  }

  private ADDFHelper mContainer;

  /**
   * @return the {@link ADDFHelper} that contains this handler
   */
  public ADDFHelper getContainer() {
    return mContainer;
  }

  /**
   * @param aContainer
   *          the containing {@link ADDFHelper} to set
   * 
   * @return this instance, for call-chaining style
   */
  public ADDFFunctionalGroupHandler setContainer(ADDFHelper aContainer) {
    this.mContainer = aContainer;
    return this;
  }

  @Override
  // ISupportPhantomReference
  public void cleanup() {
    this.setContainer(null);
  }
}
