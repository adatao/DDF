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

  public ADDFFunctionalGroupHandler(ADDFImplementor theContainer) {
    this.setContainer(theContainer);
  }


  /**
   * @return the {@link DDF} this handler handles, via the Container
   */
  public DDF getDDF() {
    return this.getContainer().getDDF();
  }

  private ADDFImplementor mContainer;

  /**
   * @return the {@link ADDFImplementor} that contains this handler
   */
  public ADDFImplementor getContainer() {
    return mContainer;
  }

  /**
   * @param aContainer
   *          the containing {@link ADDFImplementor} to set
   * 
   * @return this instance, for call-chaining style
   */
  public ADDFFunctionalGroupHandler setContainer(ADDFImplementor aContainer) {
    this.mContainer = aContainer;
    return this;
  }

}
