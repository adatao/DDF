/**
 * 
 */
package com.adatao.ddf;

/**
 * Abstract base class for a handler of a group of DDF functionality, e.g.,
 * {@link IHandleMetadata}, or {@link IHandleFilteringAndProjections}.
 * 
 * @author ctn
 * 
 */
public class ADDFFunctionalGroupHandler {

  public ADDFFunctionalGroupHandler(ADDFHelper theContainer) {
    this.setContainer(theContainer);
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

}
