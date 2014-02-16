/**
 * 
 */
package com.adatao.ddf.etl;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;

/**
 * @author ctn
 * 
 */
public abstract class ADataCommandHandler extends ADDFFunctionalGroupHandler implements IHandleDataCommands {

  public ADataCommandHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
  }

  protected void resetRepresentationsAndViews() {
    // this.getDDF().getHelper().getRepresentationHandler().reset();
    // this.getDDF().getHelper().getViewHandler().reset();
  }
}
