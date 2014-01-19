/**
 * 
 */
package com.adatao.ddf;

/**
 * Abstract base class for a handler of a group of DataFrame functionality, e.g.,
 * {@link IHandleMetadata}, or {@link IHandleFilteringAndProjections}.
 * 
 * @author ctn
 * 
 */
public class ADataFrameFunctionalGroupHandler {

  public ADataFrameFunctionalGroupHandler(ADataFrameImplementor theContainer) {
    this.setContainer(theContainer);
  }


  /**
   * @return the {@link DDF} this handler handles, via the Container
   */
  public DDF getDataFrame() {
    return this.getContainer().getDataFrame();
  }

  private ADataFrameImplementor mContainer;

  /**
   * @return the {@link ADataFrameImplementor} that contains this handler
   */
  public ADataFrameImplementor getContainer() {
    return mContainer;
  }

  /**
   * @param aContainer
   *          the containing {@link ADataFrameImplementor} to set
   * 
   * @return this instance, for call-chaining style
   */
  public ADataFrameFunctionalGroupHandler setContainer(ADataFrameImplementor aContainer) {
    this.mContainer = aContainer;
    return this;
  }

}
