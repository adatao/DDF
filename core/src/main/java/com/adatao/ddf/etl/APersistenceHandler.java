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
public abstract class APersistenceHandler extends ADDFFunctionalGroupHandler implements IHandlePersistence {

  public APersistenceHandler(ADDFManager theContainer) {
    super(theContainer);
  }

  protected void resetRepresentationsAndViews() {
    // this.getDDF().getHelper().getRepresentationHandler().reset();
    // this.getDDF().getHelper().getViewHandler().reset();
  }
}
